import pytest
from pydantic import ValidationError

from resource_bridge.config import BridgeConfig, ResourceBridgeConfig


def test_default_config_is_idle():
    config = ResourceBridgeConfig()
    assert config.bridges == [] and config.presentation_service == 'platform.presentation' and config.driver == 'platform.driver'


def test_bridge_uais_accept_lists_and_slash_strings_and_default_their_key():
    bridge = BridgeConfig(source='site1/pv_inverter', target=['site1', 'pv_sep2'])
    assert bridge.source.uai == ['site1', 'pv_inverter'] and bridge.target.uai == ['site1', 'pv_sep2'] and not bridge.target.bus
    assert bridge.key == 'site1/pv_inverter>site1/pv_sep2' and bridge.enabled and bridge.points is None and not bridge.write_unchanged
    assert BridgeConfig(name='pv', source='a', target='b').key == 'pv'


def test_bad_uais_and_unknown_keys_are_rejected():
    with pytest.raises(ValidationError):
        BridgeConfig(source='', target='b')
    with pytest.raises(ValidationError):
        BridgeConfig(source=['a'], target=['b'], direction='control')


def test_bridge_keys_must_be_unique():
    with pytest.raises(ValidationError):
        ResourceBridgeConfig(bridges=[{'source': 'a', 'target': 'b'}, {'source': 'a', 'target': 'b'}])
    config = ResourceBridgeConfig(bridges=[{'source': 'a', 'target': 'b'}, {'name': 'again', 'source': 'a', 'target': 'b'}], echo_window=0)
    assert [b.key for b in config.bridges] == ['a>b', 'again'] and config.echo_window == 0


def test_bus_ends():
    bridge = BridgeConfig(source={'uai': 'openfmb/solarmodule/SolarControlProfile/m1', 'bus': True}, target='site1/pv')
    assert bridge.source.bus and bridge.source.bus_topic == 'openfmb/solarmodule/SolarControlProfile/m1'
    assert bridge.key == 'openfmb/solarmodule/SolarControlProfile/m1>site1/pv'
    custom = BridgeConfig(source='site1/pv', target={'uai': ['openfmb', 'x'], 'bus': True, 'topic': 'elsewhere/x'})
    assert custom.target.bus_topic == 'elsewhere/x'
    with pytest.raises(ValidationError):
        BridgeConfig(source={'uai': 'a', 'bus': 'maybe', 'extra': 1}, target='b')
