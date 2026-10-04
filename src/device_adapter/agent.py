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
"""The Device Adapter Agent: bridges one VOLTTRON device into another across protocols.

A *bridge* takes the publications of a source device (its ``devices/<device>/all`` polls and the ``/multi`` pushes a
served device receives from its peer), asks the presentation service for the chain that turns a message in the source's
format into the target device's own points, scored on the fields the message carries, executes it, and writes the
result to the target through the platform driver. The service only answers mapping and transformation questions; this
agent owns the subscriptions, the writes and the policy around them (change-only writes, echo suppression, ordered
curve batches, per-point error reporting).

Telemetry into a served device (a 2030.5 server, a DNP3 outstation, a Modbus unit the platform serves) and control into
a client-role device (a real inverter behind the Modbus master, a real outstation behind the DNP3 master) are the same
operation here: a source publication becomes a write. Which points the target accepts is the target interface's
business; refused points come back as errors.
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

from interoperability.transform_parser import TransformParser

from .config import BridgeConfig, DeviceAdapterConfig
from .writer import DeviceWriter, SEQUENCE, WriteResult

_log = logging.getLogger(__name__)
__version__ = '2.0.0rc0'

ALL, MULTI = 'all', 'multi'


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
class Bridge:
    """A configured bridge after both ends resolved."""
    config: BridgeConfig
    source_format: str
    source_topics: list[str]           # the publications subscribed to
    target_format: str
    rpc_topic: str
    parameters: dict = field(default_factory=dict)
    chains: 'OrderedDict[frozenset, Chain]' = field(default_factory=OrderedDict)
    callbacks: list = field(default_factory=list)

    @property
    def key(self) -> str:
        return self.config.key

    @property
    def source_device(self) -> str:
        return device_of(self.source_topics[0])

    @property
    def target_device(self) -> str:
        return device_of(self.rpc_topic)

    def describe(self) -> dict:
        return {'name': self.key, 'source': self.config.source, 'target': self.config.target, 'source_format': self.source_format,
                'source_topics': list(self.source_topics), 'target_format': self.target_format, 'rpc_topic': self.rpc_topic,
                'enabled': self.config.enabled, 'chains': [list(c.path) for c in self.chains.values()]}


class DeviceAdapterAgent(Agent):
    """Mirrors device publications into other devices through the presentation service's chains and the driver."""

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
        for bridge_config in self.config.bridges:
            if bridge_config.enabled:
                self.core.spawn(self._start_bridge, bridge_config)
        if getattr(self.core, 'connected', False):
            self.vip.health.set_status(STATUS_GOOD, f'{len(self.config.bridges)} bridge(s) configured.')

    def _resolve(self, uai: list[str], **kwargs) -> dict:
        return self._rpc(self.config.presentation_service, 'resolve', list(uai), **kwargs) or {}

    def _start_bridge(self, config: BridgeConfig) -> Bridge | None:
        try:
            source = self._resolve(config.source)
            target = self._resolve(config.target)
        except Exception as e:
            self._report_bad_status(f'Bridge {config.key}: unable to resolve its ends through {self.config.presentation_service}: {e!r}')
            return None
        if not source.get('publication_topic'):
            self._report_bad_status(f'Bridge {config.key}: {"/".join(config.source)} does not lead to a canonical resource with'
                                    f' a publication_topic; nothing to bridge.')
            return None
        if not target.get('rpc_topic'):
            self._report_bad_status(f'Bridge {config.key}: {"/".join(config.target)} does not lead to a canonical resource with'
                                    f' an rpc_topic; it cannot be written.')
            return None
        device_topic = source['publication_topic'].rstrip('/')
        if device_topic.endswith(f'/{ALL}'):
            device_topic = device_topic[:-len(ALL) - 1]
        bridge = Bridge(config, source['data_format'], [f'{device_topic}/{ALL}', f'{device_topic}/{MULTI}'],
                        target['data_format'], target['rpc_topic'], dict(target.get('parameters') or {}))
        for topic in bridge.source_topics:
            def handle(peer, sender, bus, published_topic, headers, message, _bridge=bridge):
                self.relay(_bridge, published_topic, message)
            self.vip.pubsub.subscribe(peer='pubsub', prefix=topic, callback=handle)
            bridge.callbacks.append((topic, handle))
        self.bridges[bridge.key] = bridge
        _log.info(f'Bridge {bridge.key}: {bridge.source_topics[0]} ({bridge.source_format}) -> {bridge.rpc_topic} ({bridge.target_format}).')
        return bridge

    def _stop_bridge(self, bridge: Bridge):
        for topic, handle in bridge.callbacks:
            try:
                self.vip.pubsub.unsubscribe(peer='pubsub', prefix=topic, callback=handle)
            except Exception as e:                                                    # pragma: no cover
                _log.warning(f'Bridge {bridge.key}: unable to unsubscribe from {topic}: {e}')
        self.bridges.pop(bridge.key, None)

    # ---- relaying ------------------------------------------------------------------------------------------------
    def relay(self, bridge: Bridge, topic: str, message) -> WriteResult | None:
        """Bridge one publication of the source: transform it into the target's points and write them."""
        values = message_values(message)
        if values is None:
            _log.warning(f'Bridge {bridge.key}: ignoring a publication on {topic} that is not a device message: {type(message).__name__}')
            return None
        if bridge.config.points:
            values = {k: v for k, v in values.items() if k in bridge.config.points}
        values = self._without_echoes(bridge.source_device, values)
        if not values:
            return None
        try:
            chain = self._chain_for(bridge, values)
            out = chain.transform.execute(values)
        except Exception as e:
            _log.warning(f'Bridge {bridge.key}: unable to transform {len(values)} points from {topic}: {e!r}')
            return None
        if not isinstance(out, dict):
            _log.warning(f'Bridge {bridge.key}: the chain produced {type(out).__name__}, not a device message; nothing written.')
            return None
        if not bridge.config.write_unchanged:
            out = self._only_changes(bridge.target_device, out)
        if not out:
            return None
        result = self.writer.write(bridge.rpc_topic, out)
        self._remember(bridge.target_device, result.written)
        self.vip.pubsub.publish('pubsub', topic=f'{self.config.result_topic_prefix}/{bridge.key}',
                                message={'source': topic, 'target': bridge.rpc_topic, 'path': list(chain.path),
                                         'retention': chain.retention, **result.as_dict()})
        if result.errors:
            _log.warning(f'Bridge {bridge.key}: {len(result.written)} written, {len(result.errors)} refused: '
                         f'{dict(list(result.errors.items())[:3])}')
        return result

    def _chain_for(self, bridge: Bridge, values: dict) -> Chain:
        """The chain for this shape of message (its fields), from the service; one per distinct field set is kept."""
        fields = leaf_fields(values)
        key = frozenset(fields)
        if key in bridge.chains:
            bridge.chains.move_to_end(key)
            return bridge.chains[key]
        resolved = self._resolve(bridge.config.target, as_format=bridge.source_format, direction='write', fields=fields)
        parser = TransformParser(context={**bridge.parameters, **(resolved.get('parameters') or {})})
        chain = Chain(parser.build_transform_from_schema(resolved.get('transform') or []),
                      list(resolved.get('path') or [bridge.source_format, bridge.target_format]), float(resolved.get('retention', 1.0)))
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
        return [active.get(c.key, {'name': c.key, 'source': c.source, 'target': c.target, 'enabled': c.enabled, 'active': False})
                for c in self.config.bridges]

    @RPC.export
    def relay_message(self, bridge: str, message) -> dict:
        """Push one device message through a bridge as if its source had published it."""
        found = self.bridges.get(bridge)
        if found is None:
            raise ValueError(f'No active bridge named {bridge!r}; active: {sorted(self.bridges)}')
        result = self.relay(found, found.source_topics[0], message)
        return result.as_dict() if result else {'written': {}, 'errors': {}, 'batches': 0, 'failed_batch': None}

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
    vip_main(DeviceAdapterAgent, identity='platform.device_adapter', version=__version__)


if __name__ == '__main__':
    sys.exit(main())
