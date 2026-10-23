# -*- coding: utf-8 -*- {{{
# ===----------------------------------------------------------------------===
#
#                 Installable Component of Eclipse VOLTTRON
#
# ===----------------------------------------------------------------------===
#
# Copyright 2025 Battelle Memorial Institute
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# ===----------------------------------------------------------------------===
# }}}
"""The Device Adapter Agent: bridges one VOLTTRON device into another, or into OpenFMB on the VOLTTRON bus.

A *bridge* has a source and a target. A device end is a device behind the platform driver: read from its publications
(the ``devices/<device>/all`` polls and the ``/multi`` pushes a served device receives from its peer), written through
``set_multiple_points``. A bus end is a topic on VOLTTRON's own bus carrying messages in the end's alias format, an
OpenFMB profile topic (``openfmb/<module>/<Profile>/<mRID>``) in practice: read from it, or published to it with the
headers of ``interoperability.openfmb_bus``. The presentation service supplies the chain between the two formats,
scored on the fields each message carries; this agent owns the subscriptions, the writes and publishes, and the policy
(change-only writes, echo suppression, ordered curve batches, per-point error reporting).

Telemetry into a served device (a 2030.5 server, a DNP3 outstation, a Modbus unit the platform serves), control into a
client-role device (a real inverter behind the Modbus master) and OpenFMB profiles in either direction are the same
operation here. Which points a device accepts is its interface's business; refused points come back as errors.
"""
import logging
import sys
import time

from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any

from pydantic import ValidationError

from volttron.client import Agent
from volttron.client.messaging.health import STATUS_BAD, STATUS_GOOD
from volttron.client.vip.agent import RPC
from volttron.utils import load_config, vip_main

from interoperability import openfmb_bus
from interoperability.transform_parser import TransformParser

from .config import BridgeConfig, DeviceAdapterConfig, EndConfig
from .writer import DeviceWriter, SEQUENCE, WriteResult

_log = logging.getLogger(__name__)
__version__ = '2.0.0rc0'

ALL, MULTI = 'all', 'multi'
DEFAULT_IDENTITY = 'platform.device_adapter'


def message_values(message) -> dict | None:
    """The point values of a device publication: ``[values, meta]`` (the platform driver) or a bare ``{point: value}``."""
    if isinstance(message, (list, tuple)) and message and isinstance(message[0], dict):
        return dict(message[0])
    if isinstance(message, dict):
        return dict(message)
    return None


def leaf_fields(value, prefix: tuple = ()) -> list[str]:
    """Dotted leaf paths of a message (list indices as digits): the field hint for the service's chain scorer."""
    if isinstance(value, dict):
        return [f for k, v in value.items() for f in leaf_fields(v, prefix + (str(k),))]
    if isinstance(value, list):
        return [f for i, v in enumerate(value) for f in leaf_fields(v, prefix + (str(i),))]
    return ['.'.join(prefix)] if prefix else []


def same_value(a, b) -> bool:
    if a == b:
        return True
    if isinstance(a, (int, float)) and isinstance(b, (int, float)) and not isinstance(a, bool) and not isinstance(b, bool):
        return abs(a - b) <= 1e-9 * max(1.0, abs(a), abs(b))
    return False


def device_of(topic: str) -> str:
    """The device topic behind a publication topic, without the ``devices/`` root: the key both publication topics and
    rpc topics reduce to."""
    topic = topic.strip('/')
    for suffix in (f'/{ALL}', f'/{MULTI}'):
        if topic.endswith(suffix):
            topic = topic[:-len(suffix)]
    return topic[len('devices/'):] if topic.startswith('devices/') else topic


@dataclass
class Chain:
    transform: Any
    path: list[str]
    retention: float


@dataclass
class End:
    """A bridge end after resolving: what to subscribe to or write/publish, and the format of its messages."""
    config: EndConfig
    format: str
    topics: list[str]                       # subscribed to (source) or published on (bus target)
    rpc_topic: str | None = None            # device target: where set_multiple_points goes
    proto: str | None = None                # bus end: the protobuf message name for the headers
    parameters: dict = field(default_factory=dict)

    @property
    def bus(self) -> bool:
        return self.config.bus

    @property
    def key(self) -> str:
        """Echo and change bookkeeping key: the device behind a device end, the topic of a bus end."""
        return self.topics[0] if self.bus else device_of(self.rpc_topic or self.topics[0])


@dataclass
class Bridge:
    config: BridgeConfig
    source: End
    target: End
    chains: 'OrderedDict[frozenset, Chain]' = field(default_factory=OrderedDict)
    callbacks: list = field(default_factory=list)
    stopped: bool = False       # set by _stop_bridge; a late message on a subscription not yet dropped is ignored

    @property
    def key(self) -> str:
        return self.config.key

    def describe(self) -> dict:
        return {'name': self.key, 'source': self.config.source.uai, 'target': self.config.target.uai, 'enabled': self.config.enabled,
                'active': True, 'source_format': self.source.format, 'source_topics': list(self.source.topics), 'source_bus': self.source.bus,
                'target_format': self.target.format, 'target_bus': self.target.bus,
                'target_topic': self.target.topics[0] if self.target.bus else self.target.rpc_topic,
                'chains': [list(c.path) for c in self.chains.values()]}


class DeviceAdapterAgent(Agent):
    """Mirrors device publications into other devices, or OpenFMB profiles on the bus, through the service's chains."""

    def __init__(self, config_path: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self.config = DeviceAdapterConfig()
        self.bridges: dict[str, Bridge] = {}
        self.writer = DeviceWriter(self._rpc, self.config.driver)
        # Values this agent wrote, by device then point: a publication that merely confirms one of them within
        # echo_window is not bridged back, which keeps a pair of bridges between two devices from looping.
        self._recent_writes: dict[str, dict[str, tuple[Any, float]]] = {}
        self._last_written: dict[str, dict[str, Any]] = {}
        if config_path:
            self.vip.config.set_default('config', load_config(config_path))
        self.vip.config.subscribe(self.configure_main, ['NEW', 'UPDATE'], 'config')

    @property
    def identity(self) -> str:
        identity = getattr(self.core, 'identity', None)
        return identity if isinstance(identity, str) else DEFAULT_IDENTITY

    # ---- configuration -------------------------------------------------------------------------------------------
    def _rpc(self, peer: str, method: str, *args, **kwargs):
        return self.vip.rpc.call(peer, method, *args, **kwargs).get(timeout=self.config.rpc_timeout)

    def _report_bad_status(self, message: str):
        _log.error(message)
        if getattr(self.core, 'connected', False):
            self.vip.health.set_status(STATUS_BAD, message)

    def configure_main(self, _, action: str, contents: dict):
        try:
            new_config = DeviceAdapterConfig(**contents)
        except (ValidationError, TypeError, ValueError) as e:
            self._report_bad_status(f'Validation of the device adapter configuration failed: {e}')
            return
        for bridge in list(self.bridges.values()):
            self._stop_bridge(bridge)
        self.config = new_config
        self.writer = DeviceWriter(self._rpc, self.config.driver)
        self._warn_shared_targets(self.config.bridges)
        for bridge_config in self.config.bridges:
            if bridge_config.enabled:
                self.core.spawn(self._start_bridge, bridge_config)
        if getattr(self.core, 'connected', False):
            self.vip.health.set_status(STATUS_GOOD, f'{len(self.config.bridges)} bridge(s) configured.')

    def _warn_shared_targets(self, bridges: list[BridgeConfig]):
        """Two enabled bridges into one target may both produce the same target point; if their sources disagree the
        target takes whichever wrote last. Which points, if any, overlap depends on the transforms, so the operator is
        told about the shared target and decides."""
        by_target: dict[str, list[str]] = {}
        for bridge in bridges:
            if bridge.enabled and not bridge.target.bus:
                by_target.setdefault(bridge.target.key, []).append(bridge.key)
        for target, names in by_target.items():
            if len(names) > 1:
                _log.warning(f'Bridges {", ".join(names)} all write to {target}: a point that more than one of them'
                             f' produces holds the value of whichever source reported last.')

    def _resolve(self, uai: list[str], **kwargs) -> dict:
        return self._rpc(self.config.presentation_service, 'resolve', list(uai), **kwargs) or {}

    def _resolve_end(self, bridge: str, end: EndConfig, role: str) -> End | None:
        resolved = self._resolve(end.uai)
        if not resolved:
            self._report_bad_status(f'Bridge {bridge}: {end.key} leads to no resource the presentation service knows.')
            return None
        if end.bus:
            # The topic carries messages in the UAI's own (alias) format; a canonical UAI's format is the device's.
            fmt = resolved.get('target_format') or resolved['data_format']
            proto = (resolved.get('codec') or {}).get('proto')
            return End(end, fmt, [end.bus_topic], None, proto, dict(resolved.get('parameters') or {}))
        if role == 'source':
            if not resolved.get('publication_topic'):
                self._report_bad_status(f'Bridge {bridge}: {end.key} leads to a resource without a publication_topic; nothing to bridge.')
                return None
            device = resolved['publication_topic'].rstrip('/')
            if device.endswith(f'/{ALL}'):
                device = device[:-len(ALL) - 1]
            return End(end, resolved['data_format'], [f'{device}/{ALL}', f'{device}/{MULTI}'], resolved.get('rpc_topic'))
        if not resolved.get('rpc_topic'):
            self._report_bad_status(f'Bridge {bridge}: {end.key} leads to a resource without an rpc_topic; it cannot be written.')
            return None
        return End(end, resolved['data_format'], [], resolved['rpc_topic'], None, dict(resolved.get('parameters') or {}))

    def _start_bridge(self, config: BridgeConfig) -> Bridge | None:
        try:
            source = self._resolve_end(config.key, config.source, 'source')
            target = self._resolve_end(config.key, config.target, 'target') if source else None
        except Exception as e:
            self._report_bad_status(f'Bridge {config.key}: unable to resolve its ends through {self.config.presentation_service}: {e!r}')
            return None
        if source is None or target is None:
            return None
        bridge = Bridge(config, source, target)
        for topic in source.topics:
            def handle(peer, sender, bus, published_topic, headers, message, _bridge=bridge):
                if not _bridge.stopped:
                    self.relay(_bridge, published_topic, message, headers)
            self.vip.pubsub.subscribe(peer='pubsub', prefix=topic, callback=handle)
            bridge.callbacks.append((topic, handle))
        self.bridges[bridge.key] = bridge
        _log.info(f'Bridge {bridge.key}: {source.topics[0]} ({source.format}) -> '
                  f'{target.topics[0] if target.bus else target.rpc_topic} ({target.format}).')
        return bridge

    def _stop_bridge(self, bridge: Bridge):
        bridge.stopped = True               # from here on its handlers ignore messages, whatever the unsubscribe does
        for topic, handle in bridge.callbacks:
            try:
                self.vip.pubsub.unsubscribe(peer='pubsub', prefix=topic, callback=handle)
            except Exception as e:                                                    # pragma: no cover
                _log.warning(f'Bridge {bridge.key}: unable to unsubscribe from {topic}: {e!r}')
        self.bridges.pop(bridge.key, None)

    # ---- relaying ------------------------------------------------------------------------------------------------
    def relay(self, bridge: Bridge, topic: str, message, headers: dict | None = None) -> WriteResult | None:
        """Bridge one message of the source: transform it into the target's format and write or publish it."""
        if bridge.source.bus and openfmb_bus.origin_of(headers) == self.identity:
            return None                                                   # our own publish on a topic we also read
        values = message_values(message)
        if values is None:
            _log.warning(f'Bridge {bridge.key}: ignoring a message on {topic} that is not a mapping: {type(message).__name__}')
            return None
        if not bridge.source.bus:
            if bridge.config.points:
                values = {k: v for k, v in values.items() if k in bridge.config.points}
            values = self._without_echoes(bridge.source.key, values)
        if not values:
            return None
        try:
            chain = self._chain_for(bridge, values)
            out = chain.transform.execute(values)
        except Exception as e:
            _log.warning(f'Bridge {bridge.key}: unable to transform {len(values)} fields from {topic}: {e!r}')
            return None
        if not isinstance(out, dict):
            _log.warning(f'Bridge {bridge.key}: the chain produced {type(out).__name__}, not a mapping; nothing written.')
            return None
        out = {k: v for k, v in out.items() if v is not None}   # a point without a value is not a write
        if bridge.target.bus:
            return self._publish(bridge, topic, chain, out)
        if not bridge.config.write_unchanged:
            out = self._only_changes(bridge.target.key, out)
        if not out:
            return None
        result = self.writer.write(bridge.target.rpc_topic, out)
        self._remember(bridge.target.key, result.written)
        self._report(bridge, topic, chain, {'target': bridge.target.rpc_topic, **result.as_dict()})
        if result.errors:
            _log.warning(f'Bridge {bridge.key}: {len(result.written)} written, {len(result.errors)} refused: '
                         f'{dict(list(result.errors.items())[:3])}')
        return result

    def _publish(self, bridge: Bridge, topic: str, chain: Chain, out: dict) -> WriteResult:
        """A bus target: the transformed message goes on the target's topic with the OpenFMB-on-bus headers."""
        if not out:
            return WriteResult()
        target_topic = bridge.target.topics[0]
        self.vip.pubsub.publish('pubsub', topic=target_topic, headers=openfmb_bus.headers(bridge.target.proto, self.identity), message=out)
        result = WriteResult(written={k: v for k, v in out.items()})
        self._report(bridge, topic, chain, {'target': target_topic, 'published': True, **result.as_dict()})
        return result

    def _report(self, bridge: Bridge, source_topic: str, chain: Chain, body: dict):
        self.vip.pubsub.publish('pubsub', topic=f'{self.config.result_topic_prefix}/{bridge.key}',
                                message={'source': source_topic, 'path': list(chain.path), 'retention': chain.retention, **body})

    def _chain_for(self, bridge: Bridge, values: dict) -> Chain:
        """The chain for this shape of message (its fields), from the service; one per distinct field set is kept."""
        fields = leaf_fields(values)
        key = frozenset(fields)
        if key in bridge.chains:
            bridge.chains.move_to_end(key)
            return bridge.chains[key]
        resolved = self._rpc(self.config.presentation_service, 'transform_chain', bridge.source.format, bridge.target.format, fields) or {}
        parser = TransformParser(context={**bridge.source.parameters, **bridge.target.parameters})
        chain = Chain(parser.build_transform_from_schema(resolved.get('transform') or []),
                      list(resolved.get('path') or [bridge.source.format, bridge.target.format]), float(resolved.get('retention', 1.0)))
        bridge.chains[key] = chain
        while len(bridge.chains) > self.config.chain_cache_size:
            bridge.chains.popitem(last=False)
        _log.info(f'Bridge {bridge.key}: {" -> ".join(chain.path)} retains {chain.retention:.0%} of {len(fields)} fields.')
        return chain

    def _without_echoes(self, device: str, values: dict) -> dict:
        recent = self._recent_writes.get(device)
        if not recent:
            return values
        now = time.time()
        kept = {}
        for point, value in values.items():
            written = recent.get(point)
            if written is not None and now - written[1] <= self.config.echo_window and same_value(written[0], value):
                continue                                             # our own write coming back
            kept[point] = value
        return kept

    def _only_changes(self, device: str, out: dict) -> dict:
        """What has not already been written to this target, by any bridge. The memory is deliberately per target,
        not per bridge: when two bridges feed one target values that disagree, each write of one makes the other's
        value a change again, so the target visibly alternates between them at every poll. That flapping, together
        with the shared-target warning at configuration, is how the conflict shows; a per-bridge memory would let the
        later writer win silently and hide that one source's values are being overridden."""
        last = self._last_written.get(device, {})
        return {k: v for k, v in out.items() if k == SEQUENCE or k not in last or not same_value(last[k], v)}

    def _remember(self, device: str, written: dict):
        if not written:
            return
        now = time.time()
        recent = self._recent_writes.setdefault(device, {})
        last = self._last_written.setdefault(device, {})
        for point, value in written.items():
            recent[point] = (value, now)
            last[point] = value

    # ---- RPC -----------------------------------------------------------------------------------------------------
    @RPC.export
    def list_bridges(self) -> list[dict]:
        """The configured bridges, with their resolved ends and the chains used so far."""
        active = {b.key: b.describe() for b in self.bridges.values()}
        return [active.get(c.key, {'name': c.key, 'source': c.source.uai, 'target': c.target.uai, 'enabled': c.enabled, 'active': False})
                for c in self.config.bridges]

    @RPC.export
    def relay_message(self, bridge: str, message, headers: dict | None = None) -> dict:
        """Push one message through a bridge as if its source had published it."""
        found = self.bridges.get(bridge)
        if found is None:
            raise ValueError(f'No active bridge named {bridge!r}; active: {sorted(self.bridges)}')
        result = self.relay(found, found.source.topics[0], message, headers)
        return result.as_dict() if result else WriteResult().as_dict()

    @RPC.export
    def write(self, uai: list[str], message, as_format: str | None = None) -> dict:
        """Write a protocol message to the device ``uai`` leads to: the message is in ``as_format`` (or the format of
        the outermost alias in ``uai``), is transformed into the device's own points and written. Returns the
        write result plus the chain's path and retention."""
        fields = leaf_fields(message)
        resolved = self._resolve(uai, as_format=as_format, direction='write', fields=fields)
        if not resolved.get('rpc_topic'):
            raise ValueError(f'{"/".join(uai)} does not lead to a canonical resource with an rpc_topic; it cannot be written.')
        parser = TransformParser(context=resolved.get('parameters') or {})
        out = parser.build_transform_from_schema(resolved.get('transform') or []).execute(message)
        if not isinstance(out, dict):
            raise ValueError(f'The chain to {resolved["data_format"]} produced {type(out).__name__}, not a device message.')
        result = self.writer.write(resolved['rpc_topic'], out)
        self._remember(device_of(resolved['rpc_topic']), result.written)
        return {'target': resolved['rpc_topic'], 'path': list(resolved.get('path') or []), 'retention': resolved.get('retention'),
                **result.as_dict()}


def main():
    vip_main(DeviceAdapterAgent, identity=DEFAULT_IDENTITY, version=__version__)


if __name__ == '__main__':
    sys.exit(main())
