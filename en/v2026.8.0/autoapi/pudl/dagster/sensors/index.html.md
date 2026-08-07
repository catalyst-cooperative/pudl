# pudl.dagster.sensors

Dagster sensors for PUDL.

This module defines sensor-based automation that watches the state of the PUDL code
location and requests follow-on work when specific conditions are met. Add sensor
definitions here when they poll Dagster state, external state, or partition progress to
trigger a run, rather than when the logic belongs inside an asset or job itself. Keep
the module focused on automation entrypoints and their shared defaults.

For the underlying Dagster concept, see [https://docs.dagster.io/guides/automate/sensors](https://docs.dagster.io/guides/automate/sensors)

## Attributes

| [`ferceqr_sensor_status`](#pudl.dagster.sensors.ferceqr_sensor_status)   |    |
|--------------------------------------------------------------------------|----|
| [`ferceqr_failure_sensor`](#pudl.dagster.sensors.ferceqr_failure_sensor) |    |
| [`ferceqr_success_sensor`](#pudl.dagster.sensors.ferceqr_success_sensor) |    |
| [`default_sensors`](#pudl.dagster.sensors.default_sensors)               |    |

## Module Contents

### pudl.dagster.sensors.ferceqr_sensor_status

### pudl.dagster.sensors.ferceqr_failure_sensor

### pudl.dagster.sensors.ferceqr_success_sensor

### pudl.dagster.sensors.default_sensors
