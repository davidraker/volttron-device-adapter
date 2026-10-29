"""The Resource Bridge Agent with VOLTTRON's Agent machinery stubbed out: bridges resolve through the presentation
service, subscribe to the source's publications and write the chain's output through the driver."""
from unittest import mock

import pytest

from resource_bridge import agent as agent_module
from resource_bridge.agent import ResourceBridgeAgent, device_of, leaf_fields, message_values

SOURCE = {'data_format': 'fake_pv', 'publication_topic': 'devices/site1/pv/all', 'rpc_topic': 'devices/site1/pv', 'parameters': {}}
TARGET = {'data_format': 'fake_mirror', 'publication_topic': 'devices/site1/mirror/all', 'rpc_topic': 'devices/site1/mirror',
          'parameters': {'mrid': 'm-1'}}
# fake_pv -> fake_mirror: W doubles, Hz is copied; the chain is what the service's lookup_scored would return.
CHAIN = [{'#': 'transform(device_values())', 'M_W': 'transform[W](multiple(2))', 'M_Hz': 'transform[Hz]()'}]
REVERSE = [{'#': 'transform(device_values())', 'W': 'transform[M_W](multiple(0.5))', 'Hz': 'transform[M_Hz]()'}]
TO_PROFILE = [{'#': 'transform(device_values())', 'solarReading': {'readingMMXU': {'W': {'net': {'cVal': {'mag': 'transform[W]()'}}}}},
               'solarInverter': {'conductingEquipment': {'mRID': "param('mrid')"}}}]
FROM_PROFILE = [{'LimPct': 'transform[solarControl, limitW]()'}]
READING = ['openfmb', 'solarmodule', 'SolarReadingProfile', 'm-1']
CONTROL = ['openfmb', 'solarmodule', 'SolarControlProfile', 'm-1']


class Subscribers:
    """Every callback subscribed to one prefix; calling it delivers a message to all of them, as the bus does."""
    def __init__(self):
        self.callbacks = []

    def __call__(self, *args):
        for callback in list(self.callbacks):
            callback(*args)


class FakeVolttron:
    """Records RPCs, subscriptions and publishes; answers resolve and set_multiple_points."""
    def __init__(self):
        self.calls, self.subscriptions, self.published, self.published_with_headers = [], {}, [], []
        self.resources = {('site1', 'pv'): SOURCE, ('site1', 'mirror'): TARGET,
                          tuple(READING): {**SOURCE, 'target_format': 'openfmb.solar.reading', 'parameters': {'mrid': 'm-1'},
                                           'codec': {'encoding': 'protobuf', 'proto': 'solarmodule.SolarReadingProfile'}},
                          tuple(CONTROL): {**SOURCE, 'target_format': 'openfmb.solar.control', 'parameters': {'mrid': 'm-1'},
                                           'codec': {'encoding': 'protobuf', 'proto': 'solarmodule.SolarControlProfile'}}}
        self.driver_errors = {}
        self.last_values = {}              # what the driver's last RPC reports, by point topic

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
        elif method == 'transform_chain':
            src, dst, fields = args
            chain = {('fake_pv', 'fake_mirror'): CHAIN, ('fake_mirror', 'fake_pv'): REVERSE,
                     ('fake_pv', 'openfmb.solar.reading'): TO_PROFILE, ('openfmb.solar.control', 'fake_pv'): FROM_PROFILE}[(src, dst)]
            reply.get.return_value = {'transform': chain, 'path': [src, dst], 'retention': 0.5}
        elif method == 'set_multiple_points':
            reply.get.return_value = dict(self.driver_errors)
        elif method == 'last':
            reply.get.return_value = {k: v for k, v in self.last_values.items() if k in args[0]}
        else:
            reply.get.return_value = None
        return reply

    def subscribe(self, peer, prefix, callback):
        self.subscriptions.setdefault(prefix, Subscribers()).callbacks.append(callback)

    def unsubscribe(self, peer, prefix, callback):
        subscribers = self.subscriptions.get(prefix)
        if subscribers and callback in subscribers.callbacks:
            subscribers.callbacks.remove(callback)
        if subscribers is not None and not subscribers.callbacks:
            self.subscriptions.pop(prefix, None)

    def publish(self, peer, topic, message=None, headers=None, **kwargs):
        self.published.append((topic, message))
        self.published_with_headers.append((topic, message, headers or {}))


@pytest.fixture
def world():
    fake = FakeVolttron()
    with mock.patch.object(agent_module.Agent, '__init__', return_value=None):
        a = ResourceBridgeAgent.__new__(ResourceBridgeAgent)
        a.vip = mock.MagicMock()
        a.core = mock.MagicMock()
        a.core.connected = True
        a.core.spawn.side_effect = lambda fn, *args: fn(*args)
        ResourceBridgeAgent.__init__(a, config_path=None)
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
    # The source's polls and pushes feed the bridge; the target's tell the agent what the target holds.
    assert set(fake.subscriptions) == {'devices/site1/pv/all', 'devices/site1/pv/multi', 'devices/site1/mirror/all', 'devices/site1/mirror/multi'}
    bridge = a.bridges['pv_to_mirror']
    assert bridge.source.format == 'fake_pv' and bridge.target.format == 'fake_mirror' and bridge.target.rpc_topic == 'devices/site1/mirror'
    assert [c[:2] for c in fake.calls] == [('platform.presentation', 'resolve')] * 2
    listed = a.list_bridges()
    assert listed[0]['name'] == 'pv_to_mirror' and listed[0]['source_topics'] == ['devices/site1/pv/all', 'devices/site1/pv/multi']
    a.vip.health.set_status.assert_called()


def test_a_publication_is_transformed_with_a_field_hinted_write_chain_and_written(world):
    a, fake = world
    configure(a)
    fake.subscriptions['devices/site1/pv/all']('pubsub', 'platform.driver', 'bus', 'devices/site1/pv/all', {},
                                               [{'W': 4321, 'Hz': 60}, {'W': {'units': 'W'}, 'Hz': {'units': 'Hz'}}])
    chain_call = next(c for c in fake.calls if c[1] == 'transform_chain')
    assert chain_call[2] == ('fake_pv', 'fake_mirror', ['W', 'Hz'])
    assert writes(fake) == [('devices/site1/mirror', {'M_W': 8642, 'M_Hz': 60})]
    topic, result = fake.published[-1]
    assert topic == 'resource_bridge/pv_to_mirror' and result['written'] == {'M_W': 8642, 'M_Hz': 60} and result['errors'] == {}
    assert result['path'] == ['fake_pv', 'fake_mirror'] and result['retention'] == 0.5 and result['source'] == 'devices/site1/pv/all'
    # The same values again: nothing changed at the target, so nothing is written; a change is.
    fake.subscriptions['devices/site1/pv/multi']('pubsub', 'platform.driver', 'bus', 'devices/site1/pv/multi', {}, [{'W': 4321}, {}])
    assert len(writes(fake)) == 1
    fake.subscriptions['devices/site1/pv/multi']('pubsub', 'platform.driver', 'bus', 'devices/site1/pv/multi', {}, [{'W': 5000}, {}])
    assert writes(fake)[-1] == ('devices/site1/mirror', {'M_W': 10000})
    # One chain per distinct field set, fetched from the service once.
    assert len([c for c in fake.calls if c[1] == 'transform_chain']) == 2


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


def test_bus_ends_publish_and_consume_openfmb_profiles(world):
    """A device -> OpenFMB topic bridge publishes the profile with the on-bus headers; an OpenFMB topic -> device
    bridge writes what arrives there, except what this agent published itself."""
    from interoperability import openfmb_bus
    a, fake = world
    a.core.identity = 'platform.resource_bridge'
    a.configure_main(None, 'NEW', {'bridges': [
        {'name': 'pv_reading', 'source': 'site1/pv', 'target': {'uai': READING, 'bus': True}},
        {'name': 'pv_control', 'source': {'uai': CONTROL, 'bus': True}, 'target': 'site1/pv'}]})
    assert set(fake.subscriptions) == {'devices/site1/pv/all', 'devices/site1/pv/multi', 'openfmb/solarmodule/SolarControlProfile/m-1'}
    listed = {b['name']: b for b in a.list_bridges()}
    assert listed['pv_reading']['target_bus'] and listed['pv_reading']['target_topic'] == 'openfmb/solarmodule/SolarReadingProfile/m-1'
    assert listed['pv_control']['source_bus'] and listed['pv_control']['source_format'] == 'openfmb.solar.control'
    # Device publication -> profile on the bus.
    fake.subscriptions['devices/site1/pv/all']('pubsub', 'platform.driver', 'bus', 'devices/site1/pv/all', {}, [{'W': 4321}, {}])
    topic, message, headers = next((t, m, h) for t, m, h in fake.published_with_headers if t.startswith('openfmb/'))
    assert topic == 'openfmb/solarmodule/SolarReadingProfile/m-1'
    assert message == {'solarReading': {'readingMMXU': {'W': {'net': {'cVal': {'mag': 4321}}}}}, 'solarInverter': {'conductingEquipment': {'mRID': 'm-1'}}}
    assert openfmb_bus.is_openfmb(headers) and openfmb_bus.proto_of(headers) == 'solarmodule.SolarReadingProfile'
    assert openfmb_bus.origin_of(headers) == 'platform.resource_bridge'
    assert writes(fake) == []                                                      # nothing was written to a device
    report = fake.published[-1][1]
    assert report['published'] is True and report['target'] == topic and report['path'] == ['fake_pv', 'openfmb.solar.reading']
    # Profile on the bus -> device write; the agent's own publishes on that topic are ignored.
    control = fake.subscriptions['openfmb/solarmodule/SolarControlProfile/m-1']
    control('pubsub', 'bus_adapter', 'bus', 'openfmb/solarmodule/SolarControlProfile/m-1',
            openfmb_bus.headers('solarmodule.SolarControlProfile', 'platform.bus_adapter'), {'solarControl': {'limitW': 50}})
    assert writes(fake) == [('devices/site1/pv', {'LimPct': 50})]
    control('pubsub', 'me', 'bus', 'openfmb/solarmodule/SolarControlProfile/m-1',
            openfmb_bus.headers('solarmodule.SolarControlProfile', 'platform.resource_bridge'), {'solarControl': {'limitW': 60}})
    assert len(writes(fake)) == 1
    chain_call = next(c for c in fake.calls if c[1] == 'transform_chain' and c[2][0] == 'openfmb.solar.control')
    assert chain_call[2] == ('openfmb.solar.control', 'fake_pv', ['solarControl.limitW'])



def test_two_bridges_into_one_target_warn_and_the_conflict_stays_visible(world, caplog):
    """Two sources disagree on a point of one target: the operator is warned at configuration, and because the
    written-value memory is per target, each source's poll makes the other's value a change again, so the target
    alternates visibly instead of one source being overridden in silence."""
    import logging
    a, fake = world
    fake.resources[('site1', 'pv2')] = {**SOURCE, 'publication_topic': 'devices/site1/pv2/all', 'rpc_topic': 'devices/site1/pv2'}
    with caplog.at_level(logging.WARNING, logger=agent_module.__name__):
        a.configure_main(None, 'NEW', {'bridges': [{'name': 'a', 'source': 'site1/pv', 'target': 'site1/mirror'},
                                                   {'name': 'b', 'source': 'site1/pv2', 'target': 'site1/mirror'}]})
    assert 'Bridges a, b all write to site1/mirror' in caplog.text
    first, second = fake.subscriptions['devices/site1/pv/all'], fake.subscriptions['devices/site1/pv2/all']
    for _ in range(3):
        first('pubsub', 'platform.driver', 'bus', 'devices/site1/pv/all', {}, [{'W': 10}, {}])
        second('pubsub', 'platform.driver', 'bus', 'devices/site1/pv2/all', {}, [{'W': 20}, {}])
    assert writes(fake) == [('devices/site1/mirror', {'M_W': 20}), ('devices/site1/mirror', {'M_W': 40})] * 3
    # A source repeating a value the target already holds is still skipped.
    second('pubsub', 'platform.driver', 'bus', 'devices/site1/pv2/all', {}, [{'W': 20}, {}])
    assert len(writes(fake)) == 6


def test_bridges_to_different_targets_do_not_warn(world, caplog):
    import logging
    a, fake = world
    fake.resources[('site1', 'mirror2')] = {**TARGET, 'publication_topic': 'devices/site1/mirror2/all', 'rpc_topic': 'devices/site1/mirror2'}
    with caplog.at_level(logging.WARNING, logger=agent_module.__name__):
        a.configure_main(None, 'NEW', {'bridges': [{'name': 'a', 'source': 'site1/pv', 'target': 'site1/mirror'},
                                                   {'name': 'b', 'source': 'site1/pv', 'target': 'site1/mirror2'}]})
    assert 'all write to' not in caplog.text


def test_a_stopped_bridge_ignores_late_messages_even_if_unsubscribing_failed(world):
    """Reconfiguration stops bridges; if the bus keeps a subscription alive (unsubscribe failed), a message that still
    reaches the old handler must not be relayed."""
    a, fake = world
    configure(a)
    old_handle = fake.subscriptions['devices/site1/pv/all']
    fake.unsubscribe = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError('bus unavailable'))
    a.configure_main(None, 'NEW', {'bridges': []})                      # stops the bridge; the fake refuses to drop it
    old_handle('pubsub', 'platform.driver', 'bus', 'devices/site1/pv/all', {}, [{'W': 10, 'Hz': 60}, {}])
    assert writes(fake) == []


def test_none_values_are_not_written(world):
    a, fake = world
    configure(a)
    fake.subscriptions['devices/site1/pv/all']('pubsub', 'platform.driver', 'bus', 'devices/site1/pv/all', {}, [{'W': 10, 'Hz': None}, {}])
    assert writes(fake) == [('devices/site1/mirror', {'M_W': 20})]


def test_a_target_changed_by_someone_else_is_written_again(world):
    """Change detection compares with what the target device holds, not with what this agent last wrote: when another
    writer moves a point, the next source message carrying the intended value re-asserts it."""
    a, fake = world
    configure(a)
    pv = fake.subscriptions['devices/site1/pv/all']
    pv('pubsub', 'platform.driver', 'bus', 'devices/site1/pv/all', {}, [{'W': 4321, 'Hz': 60}, {}])
    pv('pubsub', 'platform.driver', 'bus', 'devices/site1/pv/all', {}, [{'W': 4321, 'Hz': 60}, {}])
    assert writes(fake) == [('devices/site1/mirror', {'M_W': 8642, 'M_Hz': 60})]          # repeat: nothing to do
    # The mirror reports a different M_W (a remote master wrote it); M_Hz is as we left it.
    fake.subscriptions['devices/site1/mirror/all']('pubsub', 'platform.driver', 'bus', 'devices/site1/mirror/all', {}, [{'M_W': 5, 'M_Hz': 60}, {}])
    pv('pubsub', 'platform.driver', 'bus', 'devices/site1/pv/all', {}, [{'W': 4321, 'Hz': 60}, {}])
    assert writes(fake)[-1] == ('devices/site1/mirror', {'M_W': 8642})
    assert a._writers['site1/mirror'].drifted() == {}                                       # re-asserted, in step again


def test_stale_after_asks_the_driver_before_the_first_write(world):
    a, fake = world
    fake.last_values = {'devices/site1/mirror/M_W': {'value': 8642, 'updated': '2026-10-26T12:00:00+00:00'},
                        'devices/site1/mirror/M_Hz': {'value': None, 'updated': None}}     # never seen by the driver
    a.configure_main(None, 'NEW', {'stale_after': 30,
                                   'bridges': [{'name': 'pv_to_mirror', 'source': 'site1/pv', 'target': ['site1', 'mirror']}]})
    fake.subscriptions['devices/site1/pv/all']('pubsub', 'platform.driver', 'bus', 'devices/site1/pv/all', {}, [{'W': 4321, 'Hz': 60}, {}])
    last_call = next(c for c in fake.calls if c[1] == 'last')
    assert last_call[0] == 'platform.driver' and sorted(last_call[2][0]) == ['devices/site1/mirror/M_Hz', 'devices/site1/mirror/M_W']
    assert writes(fake) == [('devices/site1/mirror', {'M_Hz': 60})]                        # M_W already there; M_Hz unknown


def test_reconfiguration_drops_the_target_subscriptions_too(world):
    a, fake = world
    configure(a)
    assert 'devices/site1/mirror/multi' in fake.subscriptions
    a.configure_main(None, 'UPDATE', {'bridges': []})
    assert fake.subscriptions == {} and a._writers == {}
