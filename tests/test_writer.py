from unittest import mock

from device_adapter.writer import DeviceWriter


def rpc(calls, errors_by_call=None, raise_on=None):
    errors_by_call = errors_by_call or {}

    def call(peer, method, *args):
        calls.append((peer, method, *args))
        if raise_on is not None and len(calls) == raise_on:
            raise TimeoutError('driver did not answer')
        return errors_by_call.get(len(calls), {})
    return call


def test_flat_points_first_then_batches_in_order():
    calls = []
    writer = DeviceWriter(rpc(calls), driver='platform.driver')
    result = writer.write('devices/site1/der', {'AO_87': 500, 'BO_17': True,
                                                'sequence': [{'AO_244': 1, 'AO_245': 2}, {'AO_249': 95, 'AO_250': 440}]})
    assert [c[:3] for c in calls] == [('platform.driver', 'set_multiple_points', 'devices/site1/der')] * 3
    assert calls[0][3] == [('AO_87', 500), ('BO_17', True)]
    assert calls[1][3] == [('AO_244', 1), ('AO_245', 2)] and calls[2][3] == [('AO_249', 95), ('AO_250', 440)]
    assert result.written == {'AO_87': 500, 'BO_17': True, 'AO_244': 1, 'AO_245': 2, 'AO_249': 95, 'AO_250': 440}
    assert result.errors == {} and result.batches == 2 and result.failed_batch is None


def test_driver_errors_are_keyed_back_to_point_names_and_a_failed_batch_stops_the_sequence():
    calls = []
    writer = DeviceWriter(rpc(calls, errors_by_call={1: {'devices/site1/der/BO_17': 'status BLOCKED'},
                                                     2: {'devices/site1/der/AO_244': "Trying to write to a point configured read only."}}))
    result = writer.write('devices/site1/der', {'AO_87': 500, 'BO_17': True, 'sequence': [{'AO_244': 1}, {'AO_249': 95}]})
    assert result.written == {'AO_87': 500} and result.errors == {'BO_17': 'status BLOCKED',
                                                                   'AO_244': "Trying to write to a point configured read only."}
    assert result.batches == 1 and result.failed_batch == 0 and len(calls) == 2          # the second batch was never sent


def test_an_rpc_failure_fails_every_point_of_that_call_without_raising():
    calls = []
    writer = DeviceWriter(rpc(calls, raise_on=1))
    result = writer.write('devices/x', {'a': 1, 'b': 2})
    assert result.written == {} and set(result.errors) == {'a', 'b'} and 'TimeoutError' in result.errors['a']
    assert writer.write('devices/x', {'sequence': [[1, 2]]}).errors == {'sequence[0]': 'a write batch must be a non-empty mapping of points to values'}
    assert writer.write('devices/x', {}).as_dict() == {'written': {}, 'errors': {}, 'batches': 0, 'failed_batch': None}
