"""The interoperability service's end-to-end matrix with the Device Adapter doing the writes.

Runs the service's ``tests/e2e_matrix/harness.py`` (found next to the installed ``interoperability`` package, or at
``INTEROPERABILITY_SERVICE_REPO``) with ``E2E_WRITER=device_adapter``: a DeviceAdapterAgent, VOLTTRON stubbed, is
configured with one bridge per device-to-device path and every source publication is delivered to it; the service runs
in-process and the Modbus, DNP3 and 2030.5 interfaces run in both roles through real proxies. Skipped when the driver
stack is not importable. Takes about a minute.
"""
import os
import subprocess
import sys

from pathlib import Path

import pytest

for module in ('volttron.driver.interfaces.modbus.modbus', 'volttron.driver.interfaces.dnp3.dnp3',
               'volttron.driver.interfaces.ieee2030_5.ieee2030_5', 'protocol_proxy.protocol.modbus',
               'protocol_proxy.protocol.dnp3', 'protocol_proxy.protocol.ieee2030_5', 'protocol_proxy.protocol.mqtt',
               'dnp3.outstation', 'google.protobuf', 'cryptography'):
    pytest.importorskip(module)
interoperability = pytest.importorskip('interoperability')

SERVICE_REPO = Path(os.environ.get('INTEROPERABILITY_SERVICE_REPO', Path(interoperability.__file__).resolve().parents[2]))
HARNESS = SERVICE_REPO / 'tests' / 'e2e_matrix' / 'harness.py'
EXPECTED_PATHS = 24


@pytest.mark.skipif(not HARNESS.exists(), reason=f'service harness not found at {HARNESS}')
def test_device_adapter_carries_every_device_to_device_path(tmp_path):
    log = tmp_path / 'e2e.log'
    proc = subprocess.run([sys.executable, str(HARNESS), str(tmp_path / 'work')], capture_output=True, text=True, timeout=1200,
                          env={**os.environ, 'E2E_LOG': str(log), 'E2E_WRITER': 'device_adapter'})
    report = proc.stdout + proc.stderr
    if proc.returncode != 0 and log.exists():
        report += '\n--- log ---\n' + log.read_text()[-6000:]
    assert proc.returncode == 0, report
    assert 'writer: device_adapter' in proc.stdout and f'{EXPECTED_PATHS}/{EXPECTED_PATHS} passed' in proc.stdout, report
    assert proc.stdout.count('[device_adapter/') == 12, report          # every device-to-device path went through a bridge
