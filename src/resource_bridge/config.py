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
"""Configuration of the Resource Bridge: which device publications are bridged into which devices."""
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def _uai(value) -> list[str]:
    """A UAI as a list of segments; ``"site1/pv_inverter"`` is accepted for convenience."""
    if isinstance(value, str):
        value = [s for s in value.split('/') if s]
    if not isinstance(value, (list, tuple)) or not value or not all(isinstance(s, str) and s for s in value):
        raise ValueError(f'A UAI is a non-empty list of segments (or a "/"-joined string), not {value!r}.')
    return list(value)


class EndConfig(BaseModel):
    """One end of a bridge: a UAI the presentation service knows. A *device* end (the default) is the canonical device
    the UAI leads to, read from its publications or written through the driver. A *bus* end is the UAI itself as a
    topic on the VOLTTRON bus carrying messages in the UAI's (alias) format: an OpenFMB profile topic, read from or
    published to, so OpenFMB-aware agents and the Message Bus Adapter in relay mode can take part."""
    model_config = ConfigDict(validate_assignment=True, populate_by_name=True, extra='forbid')

    uai: list[str]
    bus: bool = Field(default=False, description='Treat the UAI as a topic on the VOLTTRON bus rather than as a device.')
    topic: str | None = Field(default=None, description='Bus end only: the topic, when it is not the UAI joined with "/".')

    @model_validator(mode='before')
    @classmethod
    def _bare_uai(cls, value):
        return value if isinstance(value, dict) else {'uai': value}

    @field_validator('uai', mode='before')
    @classmethod
    def _as_uai(cls, value):
        return _uai(value)

    @property
    def key(self) -> str:
        return '/'.join(self.uai)

    @property
    def bus_topic(self) -> str:
        return self.topic or '/'.join(self.uai)


class BridgeConfig(BaseModel):
    """One direction of mirroring: every message of ``source`` is transformed into the format of ``target`` (the chain
    the presentation service chooses for the fields the message carries) and written to it, or published on it."""
    model_config = ConfigDict(validate_assignment=True, populate_by_name=True, extra='forbid')

    name: str | None = Field(default=None, description='Stable handle for the bridge; defaults to "<source>><target>".')
    source: EndConfig = Field(description='Where messages come from: a device (its publications) or a bus topic.')
    target: EndConfig = Field(description='Where they go: a device (written through the driver) or a bus topic (published).')
    enabled: bool = True
    points: list[str] | None = Field(default=None, description='Only these source points are bridged (device sources; all by default).')
    write_unchanged: bool = Field(default=False, description='Write a point again even when its value is the one last'
                                                             ' written to the target (by default only changes are written).')

    @property
    def key(self) -> str:
        return self.name or f'{self.source.key}>{self.target.key}'


class ResourceBridgeConfig(BaseModel):
    model_config = ConfigDict(validate_assignment=True, populate_by_name=True, extra='forbid')

    presentation_service: str = Field(default='platform.presentation', description='VIP identity of the interoperability'
                                                                                   ' (presentation) service.')
    driver: str = Field(default='platform.driver', description='VIP identity of the platform driver that owns the targets.')
    bridges: list[BridgeConfig] = Field(default_factory=list)
    echo_window: float = Field(default=5.0, ge=0, description='Seconds during which a publication that merely confirms a'
                                                              ' value this agent wrote to that device is not bridged back.')
    rpc_timeout: float = Field(default=10.0, gt=0)
    result_topic_prefix: str = Field(default='resource_bridge', description='Each bridge publishes the outcome of every'
                                                                           ' write on <prefix>/<bridge key>.')
    chain_cache_size: int = Field(default=32, ge=1, description='Chains kept per bridge, one per distinct set of fields'
                                                                ' a publication carried.')
    stale_after: float | None = Field(default=None, ge=0,
                                      description='Before deciding whether a target point needs writing, ask the driver'
                                                  ' (its last RPC) for points whose last known value is older than this'
                                                  ' many seconds or unknown. Unset: rely on the target\'s publications'
                                                  ' and this agent\'s own writes only.')

    @model_validator(mode='after')
    def _unique_keys(self):
        keys = [b.key for b in self.bridges]
        duplicates = sorted({k for k in keys if keys.count(k) > 1})
        if duplicates:
            raise ValueError(f'Bridge keys must be unique; duplicated: {duplicates}.')
        return self
