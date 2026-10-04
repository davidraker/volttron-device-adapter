import pytest
from pydantic import ValidationError

from device_adapter.config import BridgeConfig, DeviceAdapterConfig


def test_default_config_is_idle():
    config = DeviceAdapterConfig()
    assert config.bridges == [] and config.presentation_service == 'platform.presentation' and config.driver == 'platform.driver'


def test_bridge_uais_accept_lists_and_slash_strings_and_default_their_key():
    bridge = BridgeConfig(source='site1/pv_inverter', target=['site1', 'pv_sep2'])
    assert bridge.source == ['site1', 'pv_inverter'] and bridge.target == ['site1', 'pv_sep2']
    assert bridge.key == 'site1/pv_inverter>site1/pv_sep2' and bridge.enabled and bridge.points is None and not bridge.write_unchanged
    assert BridgeConfig(name='pv', source='a', target='b').key == 'pv'


def test_bad_uais_and_unknown_keys_are_rejected():
    with pytest.raises(ValidationError):
        BridgeConfig(source='', target='b')
    with pytest.raises(ValidationError):
        BridgeConfig(source=['a'], target=['b'], direction='control')


def test_bridge_keys_must_be_unique():
    with pytest.raises(ValidationError):
        DeviceAdapterConfig(bridges=[{'source': 'a', 'target': 'b'}, {'source': 'a', 'target': 'b'}])
    config = DeviceAdapterConfig(bridges=[{'source': 'a', 'target': 'b'}, {'name': 'again', 'source': 'a', 'target': 'b'}], echo_window=0)
    assert [b.key for b in config.bridges] == ['a>b', 'again'] and config.echo_window == 0
