# Importing data from Z-Info

Importing data from [Z-Info](https://www.z-info.nl) into FlexMeasures.


## Usage

Importing sensor data:

    flexmeasures zinfo import-sensor-data


## Installation

1. Add the path to this directory to your FlexMeasures (>v0.4.0) config file,
using the FLEXMEASURES_PLUGIN_PATHS setting.

2. Add ZINFO_USERNAME, ZINFO_PASSWORD and ZINFO_SPCID to your FlexMeasures config (e.g. ~/.flexmeasures.cfg).

3. Configure the sensor IDs and their FlexMeasures counterpart:
  TODO


## Development

We use pre-commit to keep code quality up:

    pip install pre-commit black flake8 mypy
    pre-commit install
    pre-commit run --all-files --show-diff-on-failure
