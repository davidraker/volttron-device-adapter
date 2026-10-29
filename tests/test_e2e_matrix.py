"""The interoperability service's end-to-end matrix with the Resource Bridge doing the writes.

Runs the interoperability service's ``tests/e2e_matrix/harness.py`` (found next to the installed ``interoperability``
package, or at ``INTEROPERABILITY_SERVICE_REPO``) with ``E2E_WRITER=resource_bridge``: a ResourceBridgeAgent is configured with one bridge per device-to-device path
and every source publication is delivered to it; with ``E2E_OPENFMB=bus`` its bus-end bridges also carry the OpenFMB
paths over OpenFMB-on-the-VOLTTRON-bus topics, the Message Bus Adapter in relay mode re-serialising. The service runs in-process and the Modbus, DNP3 and 2030.5
interfaces run in both roles through real proxies; VOLTTRON's bus is stubbed. Skipped when the driver stack is not
importable. About a minute per mode.
"""
import os
import subprocess
import sys

from pathlib import Path

import pytest

for module in ('volttron.driver.interfaces.modbus.modbus', 'volttron.driver.interfaces.dnp3.dnp3',
               'volttron.driver.interfaces.ieee2030_5.ieee2030_5', 'protocol_proxy.protocol.modbus',
               'protocol_proxy.protocol.dnp3', 'protocol_proxy.protocol.ieee2030_5', 'protocol_proxy.protocol.mqtt',
               'dnp3.outstation', 'google.protobuf', 'cryptography', 'resource_bridge.agent', 'bus_adapter.agent'):
    pytest.importorskip(module)
interoperability = pytest.importorskip('interoperability')

SERVICE_REPO = Path(os.environ.get('INTEROPERABILITY_SERVICE_REPO', Path(interoperability.__file__).resolve().parents[2]))
HARNESS = SERVICE_REPO / 'tests' / 'e2e_matrix' / 'harness.py'
EXPECTED_PATHS = 24


@pytest.mark.skipif(not HARNESS.exists(), reason=f'service harness not found at {HARNESS}')
@pytest.mark.parametrize('openfmb_mode', ['harness', 'bus'])
def test_matrix(tmp_path, openfmb_mode):
    log = tmp_path / 'e2e.log'
    proc = subprocess.run([sys.executable, str(HARNESS), str(tmp_path / 'work')], capture_output=True, text=True, timeout=1200,
                          env={**os.environ, 'E2E_LOG': str(log), 'E2E_WRITER': 'resource_bridge', 'E2E_OPENFMB': openfmb_mode})
    report = proc.stdout + proc.stderr
    if proc.returncode != 0 and log.exists():
        report += '\n--- log ---\n' + log.read_text()[-6000:]
    assert proc.returncode == 0, report
    assert f'writer: resource_bridge; openfmb: {openfmb_mode}' in proc.stdout and f'{EXPECTED_PATHS}/{EXPECTED_PATHS} passed' in proc.stdout, report
    assert proc.stdout.count('[resource_bridge/') == 12, report          # every device-to-device path went through a bridge
    if openfmb_mode == 'bus':
        assert proc.stdout.count('via bridge') == 6 and proc.stdout.count('broker got a ') == 6, report
