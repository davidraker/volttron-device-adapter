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
"""The only code that talks to the platform driver: writes a transform chain's output to a device."""
import logging

from dataclasses import dataclass, field
from typing import Any, Callable

_log = logging.getLogger(__name__)

from interoperability.resource import SEQUENCE     # the key of ordered write batches (curve writes)


@dataclass
class WriteResult:
    written: dict[str, Any] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)
    batches: int = 0                    # ordered batches written (completely or partly)
    failed_batch: int | None = None     # index of the batch whose failure stopped the sequence

    def as_dict(self) -> dict:
        return {'written': dict(self.written), 'errors': dict(self.errors), 'batches': self.batches,
                'failed_batch': self.failed_batch}


class DeviceWriter:
    """Protocol-free by construction: it receives a device's flat points, and optionally ordered batches, and writes
    them through the driver's ``set_multiple_points`` RPC.

    Flat points go first, in one call. Then each ``sequence`` batch is its own call, in order; a failed batch stops
    the sequence (writing curve points after a failed selector would land them in the wrong curve). Errors are
    collected per point and never raise mid-sequence. ``rpc_call(peer, method, *args)`` performs a VIP RPC and returns
    its result.
    """

    def __init__(self, rpc_call: Callable[..., Any], driver: str = 'platform.driver'):
        self._call = rpc_call
        self.driver = driver

    def write(self, rpc_topic: str, points: dict[str, Any]) -> WriteResult:
        result = WriteResult()
        flat = {k: v for k, v in points.items() if k != SEQUENCE}
        batches = points.get(SEQUENCE) or []
        if flat:
            self._set(rpc_topic, flat, result)
        for index, batch in enumerate(batches):
            if not isinstance(batch, dict) or not batch:
                result.errors[f'{SEQUENCE}[{index}]'] = 'a write batch must be a non-empty mapping of points to values'
                result.failed_batch = index
                break
            before = len(result.errors)
            self._set(rpc_topic, batch, result)
            result.batches += 1
            if len(result.errors) > before:
                result.failed_batch = index
                _log.warning(f'{rpc_topic}: batch {index} of {len(batches)} failed; the remaining batches were not written.')
                break
        return result

    def _set(self, rpc_topic: str, values: dict[str, Any], result: WriteResult) -> None:
        items = list(values.items())
        try:
            errors = self._call(self.driver, 'set_multiple_points', rpc_topic, items) or {}
        except Exception as e:                       # the RPC failed as a whole: every point of this call failed
            for point in values:
                result.errors[point] = f'{type(e).__name__}: {e}'
            return
        failed = {self.point_name(key): str(error) for key, error in dict(errors).items()}
        for point, value in values.items():
            if point in failed:
                result.errors[point] = failed[point]
            else:
                result.written[point] = value

    @staticmethod
    def point_name(key: Any) -> str:
        """The driver keys errors by full point topic (``devices/<device>/<point>``); the chain speaks point names."""
        return str(key).rstrip('/').rsplit('/', 1)[-1]
