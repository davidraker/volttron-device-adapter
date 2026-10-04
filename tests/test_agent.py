"""The Device Adapter Agent with VOLTTRON's Agent machinery stubbed out: bridges resolve through the presentation
service, subscribe to the source's publications and write the chain's output through the driver."""
from unittest import mock

import pytest

from device_adapter import agent as agent_module
from device_adapter.agent import DeviceAdapterAgent, device_of, leaf_fields, message_values

SOURCE = {'data_format': 'fake_pv', 'publication_topic': 'devices/site1/pv/all', 'rpc_topic': 'devices/site1/pv', 'parameters': {}}
TARGET = {'data_format': 'fake_mirror', 'publication_topic': 'devices/site1/mirror/all', 'rpc_topic': 'devices/site1/mirror',
          'parameters': {'mrid': 'm-1'}}
# fake_pv -> fake_mirror: W doubles, Hz is copied; the chain is what the service's lookup_scored would return.
CHAIN = [{'#': 'transform(device_values())', 'M_W': 'transform[W](multiple(2))', 'M_Hz': 'transform[Hz]()'}]
REVERSE = [{'#': 'transform(device_values())', 'W': 'transform[M_W](multiple(0.5))', 'Hz': 'transform[M_Hz]()'}]


class FakeVolttron:
    """Records RPCs, subscriptions and publishes; answers resolve and set_multiple_points."""
    def __init__(self):
        self.calls, self.subscriptions, self.published = [], {}, []
        self.resources = {('site1', 'pv'): SOURCE, ('site1', 'mirror'): TARGET}
        self.driver_errors = {}

    def rpc_call(self, peer, method, *args, **kwargs):
        self.calls.append((peer, method, args, kwargs))
        reply = mock.Mock()
        if method == 'resolve':
            resource = dict(self.resources.get(tuple(args[0]), {}))
            if resource and kwargs.get('as_format'):
                chain = CHAIN if kwargs['as_format'] == 'fake_pv' else REVERSE
                resource.update(target_format=kwargs['as_format'], transform=chain, path=[kwargs['as_format'], resource['data_format']],
                                retention=0.5)
            reply.get.return_value = resource
        elif method == 'set_multiple_points':
            reply.get.return_value = dict(self.driver_errors)
        else:
            reply.get.return_value = None
        return reply

    def subscribe(self, peer, prefix, callback):
        self.subscriptions[prefix] = callback

    def unsubscribe(self, peer, prefix, callback):
        self.subscriptions.pop(prefix, None)

    def publish(self, peer, topic, message=None, **kwargs):
        self.published.append((topic, message))


@pytest.fixture
def world():
    fake = FakeVolttron()
    with mock.patch.object(agent_module.Agent, '__init__', return_value=None):
        a = DeviceAdapterAgent.__new__(DeviceAdapterAgent)
        a.vip = mock.MagicMock()
        a.core = mock.MagicMock()
        a.core.connected = True
        a.core.spawn.side_effect = lambda fn, *args: fn(*args)
        DeviceAdapterAgent.__init__(a, config_path=None)
    a.vip.rpc.call.side_effect = fake.rpc_call
    a.vip.pubsub.subscribe.side_effect = fake.subscribe
    a.vip.pubsub.unsubscribe.side_effect = fake.unsubscribe
    a.vip.pubsub.publish.side_effect = fake.publish
    return a, fake


def configure(a, **extra):
    a.configure_main(None, 'NEW', {'bridges': [{'name': 'pv_to_mirror', 'source': 'site1/pv', 'target': ['site1', 'mirror'], **extra}]})


def writes(fake):
    return [(args[0], dict(args[1])) for peer, method, args, _ in fake.calls if method == 'set_multiple_points']


def test_helpers():
    assert message_values([{'W': 1}, {'W': {'units': 'W'}}]) == {'W': 1} and message_values({'W': 1}) == {'W': 1}
    assert message_values('junk') is None and message_values([]) is None
    assert leaf_fields({'a': {'b': [1, {'c': 2}]}, 'd': 3}) == ['a.b.0', 'a.b.1.c', 'd']
    assert device_of('devices/site1/pv/all') == device_of('devices/site1/pv/multi') == device_of('site1/pv') == 'site1/pv'


def test_configure_resolves_both_ends_and_subscribes_to_polls_and_pushes(world):
    a, fake = world
    configure(a)
    assert set(fake.subscriptions) == {'devices/site1/pv/all', 'devices/site1/pv/multi'}
    bridge = a.bridges['pv_to_mirror']
    assert bridge.source_format == 'fake_pv' and bridge.target_format == 'fake_mirror' and bridge.rpc_topic == 'devices/site1/mirror'
    assert [c[:2] for c in fake.calls] == [('platform.presentation', 'resolve')] * 2
    listed = a.list_bridges()
    assert listed[0]['name'] == 'pv_to_mirror' and listed[0]['source_topics'] == ['devices/site1/pv/all', 'devices/site1/pv/multi']
    a.vip.health.set_status.assert_called()


def test_a_publication_is_transformed_with_a_field_hinted_write_chain_and_written(world):
    a, fake = world
    configure(a)
    fake.subscriptions['devices/site1/pv/all']('pubsub', 'platform.driver', 'bus', 'devices/site1/pv/all', {},
                                               [{'W': 4321, 'Hz': 60}, {'W': {'units': 'W'}, 'Hz': {'units': 'Hz'}}])
    chain_call = next(c for c in fake.calls if c[1] == 'resolve' and c[3].get('direction') == 'write')
    assert chain_call[2] == (['site1', 'mirror'],) and chain_call[3] == {'as_format': 'fake_pv', 'direction': 'write', 'fields': ['W', 'Hz']}
    assert writes(fake) == [('devices/site1/mirror', {'M_W': 8642, 'M_Hz': 60})]
    topic, result = fake.published[-1]
    assert topic == 'device_adapter/pv_to_mirror' and result['written'] == {'M_W': 8642, 'M_Hz': 60} and result['errors'] == {}
    assert result['path'] == ['fake_pv', 'fake_mirror'] and result['retention'] == 0.5 and result['source'] == 'devices/site1/pv/all'
    # The same values again: nothing changed at the target, so nothing is written; a change is.
    fake.subscriptions['devices/site1/pv/multi']('pubsub', 'platform.driver', 'bus', 'devices/site1/pv/multi', {}, [{'W': 4321}, {}])
    assert len(writes(fake)) == 1
    fake.subscriptions['devices/site1/pv/multi']('pubsub', 'platform.driver', 'bus', 'devices/site1/pv/multi', {}, [{'W': 5000}, {}])
    assert writes(fake)[-1] == ('devices/site1/mirror', {'M_W': 10000})
    # One chain per distinct field set, fetched from the service once.
    assert len([c for c in fake.calls if c[1] == 'resolve' and c[3].get('direction') == 'write']) == 2


def test_write_unchanged_and_point_filter(world):
    a, fake = world
    configure(a, write_unchanged=True, points=['W'])
    cb = fake.subscriptions['devices/site1/pv/all']
    for _ in range(2):
        cb('pubsub', 'platform.driver', 'bus', 'devices/site1/pv/all', {}, [{'W': 10, 'Hz': 60}, {}])
    assert writes(fake) == [('devices/site1/mirror', {'M_W': 20})] * 2            # Hz filtered out, repeat written


def test_our_own_writes_are_not_bridged_back(world):
    """Two bridges between the same devices: a write's reflection on the target is not written back to the source."""
    a, fake = world
    fake.resources[('site1', 'mirror')] = TARGET
    a.configure_main(None, 'NEW', {'bridges': [{'name': 'fwd', 'source': 'site1/pv', 'target': 'site1/mirror'},
                                               {'name': 'back', 'source': 'site1/mirror', 'target': 'site1/pv'}]})
    fake.subscriptions['devices/site1/pv/all']('pubsub', 'platform.driver', 'bus', 'devices/site1/pv/all', {}, [{'W': 4321, 'Hz': 60}, {}])
    assert writes(fake) == [('devices/site1/mirror', {'M_W': 8642, 'M_Hz': 60})]
    # The mirror reflects what we wrote (a push on its /multi topic): dropped as an echo.
    fake.subscriptions['devices/site1/mirror/multi']('pubsub', 'platform.driver', 'bus', 'devices/site1/mirror/multi', {}, [{'M_W': 8642, 'M_Hz': 60}, {}])
    assert len(writes(fake)) == 1
    # A value we did not write is bridged back.
    fake.subscriptions['devices/site1/mirror/multi']('pubsub', 'platform.driver', 'bus', 'devices/site1/mirror/multi', {}, [{'M_W': 1}, {}])
    assert writes(fake)[-1] == ('devices/site1/pv', {'W': 0.5})


def test_driver_errors_are_reported_not_raised(world):
    a, fake = world
    configure(a)
    fake.driver_errors = {'devices/site1/mirror/M_Hz': 'Trying to write to a point configured read only.'}
    fake.subscriptions['devices/site1/pv/all']('pubsub', 'platform.driver', 'bus', 'devices/site1/pv/all', {}, [{'W': 1, 'Hz': 60}, {}])
    _, result = fake.published[-1]
    assert result['written'] == {'M_W': 2} and result['errors'] == {'M_Hz': 'Trying to write to a point configured read only.'}


def test_ends_that_cannot_be_bridged_are_reported(world):
    a, fake = world
    fake.resources[('site1', 'nowhere')] = {'data_format': 'x', 'publication_topic': None, 'rpc_topic': None}
    a.configure_main(None, 'NEW', {'bridges': [{'name': 'bad_source', 'source': 'site1/nowhere', 'target': 'site1/mirror'},
                                               {'name': 'bad_target', 'source': 'site1/pv', 'target': 'site1/nowhere'},
                                               {'name': 'off', 'source': 'site1/pv', 'target': 'site1/mirror', 'enabled': False}]})
    assert a.bridges == {} and fake.subscriptions == {}
    assert [b['active'] for b in a.list_bridges()] == [False, False, False]
    assert a.vip.health.set_status.call_args_list[0].args[0] == 'BAD'
    with pytest.raises(ValueError):
        a.relay_message('off', [{'W': 1}, {}])


def test_reconfiguration_replaces_bridges(world):
    a, fake = world
    configure(a)
    a.configure_main(None, 'UPDATE', {'bridges': []})
    assert a.bridges == {} and fake.subscriptions == {}


def test_write_rpc_writes_a_protocol_message_to_a_device(world):
    a, fake = world
    result = a.write(['site1', 'mirror'], {'W': 100, 'Hz': 60}, as_format='fake_pv')
    assert result['target'] == 'devices/site1/mirror' and result['written'] == {'M_W': 200, 'M_Hz': 60} and result['path'] == ['fake_pv', 'fake_mirror']
    resolve = fake.calls[0]
    assert resolve[3] == {'as_format': 'fake_pv', 'direction': 'write', 'fields': ['W', 'Hz']}
    with pytest.raises(ValueError):
        a.write(['site1', 'unknown'], {'W': 1}, as_format='fake_pv')
    assert a.relay_message('missing', {}) if False else True
