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
"""Configuration of the Device Adapter: which device publications are bridged into which devices."""
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def _uai(value) -> list[str]:
    """A UAI as a list of segments; ``"site1/pv_inverter"`` is accepted for convenience."""
    if isinstance(value, str):
        value = [s for s in value.split('/') if s]
    if not isinstance(value, (list, tuple)) or not value or not all(isinstance(s, str) and s for s in value):
        raise ValueError(f'A UAI is a non-empty list of segments (or a "/"-joined string), not {value!r}.')
    return list(value)


class BridgeConfig(BaseModel):
    """One direction of mirroring: every publication of ``source`` is transformed into the format of ``target``
    (the chain the presentation service chooses for the fields the message carries) and written to it."""
    model_config = ConfigDict(validate_assignment=True, populate_by_name=True, extra='forbid')

    name: str | None = Field(default=None, description='Stable handle for the bridge; defaults to "<source>><target>".')
    source: list[str] = Field(description='UAI of the resource whose publications are bridged (its canonical resource'
                                          ' must have a publication_topic).')
    target: list[str] = Field(description='UAI of the device written (its canonical resource must have an rpc_topic).')
    enabled: bool = True
    points: list[str] | None = Field(default=None, description='Only these source points are bridged (all by default).')
    write_unchanged: bool = Field(default=False, description='Write a point again even when its value is the one last'
                                                             ' written to the target (by default only changes are written).')

    @field_validator('source', 'target', mode='before')
    @classmethod
    def _as_uai(cls, value):
        return _uai(value)

    @property
    def key(self) -> str:
        return self.name or f'{"/".join(self.source)}>{"/".join(self.target)}'


class DeviceAdapterConfig(BaseModel):
    model_config = ConfigDict(validate_assignment=True, populate_by_name=True, extra='forbid')

    presentation_service: str = Field(default='platform.presentation', description='VIP identity of the interoperability'
                                                                                   ' (presentation) service.')
    driver: str = Field(default='platform.driver', description='VIP identity of the platform driver that owns the targets.')
    bridges: list[BridgeConfig] = Field(default_factory=list)
    echo_window: float = Field(default=5.0, ge=0, description='Seconds during which a publication that merely confirms a'
                                                              ' value this agent wrote to that device is not bridged back.')
    rpc_timeout: float = Field(default=10.0, gt=0)
    result_topic_prefix: str = Field(default='device_adapter', description='Each bridge publishes the outcome of every'
                                                                           ' write on <prefix>/<bridge key>.')
    chain_cache_size: int = Field(default=32, ge=1, description='Chains kept per bridge, one per distinct set of fields'
                                                                ' a publication carried.')

    @model_validator(mode='after')
    def _unique_keys(self):
        keys = [b.key for b in self.bridges]
        duplicates = sorted({k for k in keys if keys.count(k) > 1})
        if duplicates:
            raise ValueError(f'Bridge keys must be unique; duplicated: {duplicates}.')
        return self
