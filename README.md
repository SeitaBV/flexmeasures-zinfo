# Importing data from Z-Info

Importing data from [Z-Info](https://www.z-info.nl) into FlexMeasures.


## Usage

Importing sensor data:

    flexmeasures zinfo import-sensor-data --spcid <your-zinfo-specification-id>


## Installation

1. Add the path to this directory to your FlexMeasures (>=v0.9) config file,
using the `FLEXMEASURES_PLUGIN_PATHS` setting.

2. Add `ZINFO_USERNAME`, `ZINFO_PASSWORD`, `ZINFO_EVENT_END_FIELD`, `ZINFO_EVENT_VALUE_FIELD` and `ZINFO_SENSOR_NAME_FIELD` to your FlexMeasures config (e.g. ~/.flexmeasures.cfg).

3. Configure the sensor IDs and their FlexMeasures counterpart:

   For example:

       ZINFO_MAIN_SENSORS = [
           dict(
               fm_sensor_name="<FlexMeasures sensor name>",
               generic_asset_name="<FlexMeasures generic asset name>",
               zinfo_sensor_name="<Z-info sensor name>",
               unit="kW",
               timezone="Europe/Amsterdam",
               resolution=timedelta(hours=1),
               pandas_method_kwargs=[
                   ("diff", dict()),
                   ("shift", dict(periods=-1)),
                   ("head", dict(n=-1)),
               ],
           ),
       ]

   Here, each dictionary in the list defines how to set up a FlexMeasures sensor corresponding to a Z-info sensor,
   as well as how to convert Z-info values to FlexMeasures time series.
   
   The conversion is defined using `pandas_method_kwargs`, which lists method/kwargs tuples that are called in the specified order.
   This specific example converts from hourly meter readings in kWh to electricity demand in kW. 

## Development

We use pre-commit to keep code quality up:

    pip install pre-commit black flake8 mypy
    pre-commit install
    pre-commit run --all-files --show-diff-on-failure
