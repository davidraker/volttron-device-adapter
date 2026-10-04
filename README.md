# VOLTTRON Device Adapter

The Device Adapter agent bridges one VOLTTRON device into another across protocols. In the
vocabulary of OpenFMB, an *adapter* connects a protocol-speaking device to the common bus; the
Message Bus Adapter does that between VOLTTRON and a foreign bus, and this agent does it between two
devices behind the platform driver: a SunSpec inverter and the IEEE 2030.5 server the platform
serves for it, a real DNP3 outstation and the Modbus unit a SCADA master polls, a 2030.5 utility
server's controls and the inverter they are meant for.

A *bridge* has a source and a target, both UAIs known to the presentation service. Every
publication of the source device (its `devices/<device>/all` polls and the `/multi` pushes a served
device receives from its peer) is turned into the target device's own points by the chain the
service chooses for the fields the message carries, and written to the target through the platform
driver's `set_multiple_points`. Telemetry into a served device and control into a client-role
device are the same operation; which points the target accepts is its interface's business, and
refused points come back as errors on the bridge's result topic.

The presentation service only answers mapping and transformation questions (`resolve` with
`direction='write'` and a `fields` hint). The policy lives here:

- change-only writes: a point is written when its value differs from the last one written to that
  target (`write_unchanged` turns this off);
- echo suppression: a publication that merely confirms a value this agent wrote to that device
  within `echo_window` seconds is not bridged back, so a pair of bridges between two devices does
  not loop;
- ordered curve batches: a chain output's `sequence` list is written batch by batch after the flat
  points, and a failed batch stops the sequence;
- per-point errors, never a raised exception mid-write, published with the chain's path and
  retention on `<result_topic_prefix>/<bridge key>`.

## Requirements

- `volttron-core` >= 2.0.0rc30
- `interoperability-service` (provides `interoperability.transform_parser`) running as
  `platform.presentation`, installed in the adapter's environment too
- the platform driver (`platform.driver`) owning the devices at both ends

## Configuration

```json
{
    "presentation_service": "platform.presentation",
    "driver": "platform.driver",
    "echo_window": 5.0,
    "bridges": [
        {"name": "pv_to_utility", "source": "site1/pv_inverter", "target": ["site1", "pv_sep2_server"]},
        {"name": "utility_to_pv", "source": "site1/pv_sep2_server", "target": "site1/pv_inverter",
         "points": ["DERControl_opModMaxLimW", "DERControl_opModFixedVar"]},
        {"name": "scada_view", "source": "site1/der", "target": "site1/der_modbus_view", "write_unchanged": true,
         "enabled": false}
    ]
}
```

`source` and `target` are UAIs (a list of segments, or one `/`-joined string). The source must
lead to a canonical resource with a `publication_topic`, the target to one with an `rpc_topic`.
Served devices (a server the platform runs) must be declared with their own device format,
generated with the discovery tools' `--served` flag, so the write edges into their inputs exist.

## RPC

- `list_bridges()`: the configured bridges, their resolved ends and the chains used so far.
- `relay_message(bridge, message)`: push one device message through a bridge as if its source had
  published it.
- `write(uai, message, as_format=None)`: write a protocol message (in `as_format`, or the format
  of the outermost alias in `uai`) to the device the UAI leads to. Returns the write result with the
  chain's path and retention.

## Tests

`pytest tests` runs the unit tests (VOLTTRON stubbed). `tests/test_e2e_matrix.py` runs the
interoperability service's end-to-end matrix with this agent doing the writes for every
device-to-device path; it needs the modular driver stack and the Modbus, DNP3 and 2030.5 proxies
importable and is skipped otherwise.
