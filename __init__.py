__version__ = "0.3"

import os
import sys
from datetime import datetime
from pytz import utc
from pytz.exceptions import AmbiguousTimeError
from typing import List, Tuple

import click
from flask import Blueprint, current_app
from flask.cli import with_appcontext
from flexmeasures.api.common.utils.api_utils import save_to_db
from flexmeasures.data.config import db
from flexmeasures.data.models.data_sources import DataSource
from flexmeasures.data.models.generic_assets import GenericAsset
from flexmeasures.data.models.time_series import Sensor
from flexmeasures.data.services.time_series import drop_unchanged_beliefs
from flexmeasures.data.transactional import task_with_status_report
import pandas as pd
import requests
from timely_beliefs import BeliefsDataFrame

from .utils import get_access_token


HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

zinfo_bp = Blueprint("zinfo", __name__)
ZINFO_API_BASE_URL = "https://webservice.z-info.nl/WSR"
ZINFO_TIMEZONE = "Europe/Amsterdam"
zinfo_bp.cli.help = "Z-info Data commands"


@zinfo_bp.cli.command("import-sensor-data")
@click.option(
    "--spcid",
    "zinfo_spcids",
    required=True,
    multiple=True,
    help="Which Z-info specification id to use.",
)
@click.option(
    "--dryrun/--no-dryrun",
    default=False,
    help="In Dry run, do not save the data to the db.",
)
@with_appcontext
@task_with_status_report("zinfo-import-sensor-data")
def import_sensor_data(zinfo_spcids: List[str], dryrun: bool = False):
    """
    Import sensor data from Z-info, given at least one specification ID.
    """
    access_token = get_access_token()
    zinfo_event_end_field = current_app.config.get("ZINFO_EVENT_END_FIELD", None)
    if not zinfo_event_end_field:
        click.echo("ZINFO_EVENT_END_FIELD setting is not given!")
        raise click.Abort
    zinfo_event_value_field = current_app.config.get("ZINFO_EVENT_VALUE_FIELD", None)
    if not zinfo_event_value_field:
        click.echo("ZINFO_EVENT_VALUE_FIELD setting is not given!")
        raise click.Abort
    zinfo_sensor_name_field = current_app.config.get("ZINFO_SENSOR_NAME_FIELD", None)
    if not zinfo_sensor_name_field:
        click.echo("ZINFO_SENSOR_NAME_FIELD setting is not given!")
        raise click.Abort

    for zinfo_spcid in zinfo_spcids:
        res = requests.get(
            f"{ZINFO_API_BASE_URL}/zi_wsr.svc/JSON/NL.13/?spcid={zinfo_spcid}",
            headers={"Authorization": access_token},
        )
        now = datetime.now(tz=utc)
        response = res.json()
        values = response.get("waarden", [])
        current_app.logger.info(f"Got {len(values)} values...")

        # Parse response
        df = pd.DataFrame(values)
        df = df.iloc[
            ::-1
        ]  # switch order of values so that they run from past to present
        df[zinfo_event_value_field] = pd.to_numeric(df[zinfo_event_value_field])
        df[zinfo_event_end_field] = localize_time_series(
            df[zinfo_event_end_field], ZINFO_TIMEZONE
        )
        df = df[~df[zinfo_event_end_field].isna()]
        df = (
            df.set_index([zinfo_event_end_field, zinfo_sensor_name_field])
            .sort_index()[zinfo_event_value_field]
            .to_frame()
        )

        # Convert from meter data per Z-info sensor name (e.g. meterstanden) to time series data per FlexMeasures sensor
        zinfo_main_sensors: List[dict] = current_app.config.get(
            "ZINFO_MAIN_SENSORS", {}
        )
        zinfo_sensor_mapping = {
            sensor_description["zinfo_sensor_name"]: {
                k: v for k, v in sensor_description.items() if k != "zinfo_sensor_name"
            }
            for sensor_description in zinfo_main_sensors
        }
        zinfo_sensor_names_received: List[str] = df.index.get_level_values(
            zinfo_sensor_name_field
        ).unique()
        df_sensors = []
        for zinfo_sensor_name in zinfo_sensor_names_received:
            df_sensor = df.loc[
                df.index.get_level_values(zinfo_sensor_name_field) == zinfo_sensor_name
            ]
            if zinfo_sensor_name not in zinfo_sensor_mapping:
                current_app.logger.error(
                    f"Missing Z-info sensor name {zinfo_sensor_name} in your ZINFO_MAIN_SENSORS config setting."
                )
                continue
            df_sensor = apply_pandas_method_kwargs(
                df_sensor,
                zinfo_sensor_mapping[zinfo_sensor_name]["pandas_method_kwargs"],
            )
            df_sensors.append(df_sensor)
        df = pd.concat(df_sensors, axis=0)

        if not dryrun:
            # Save main sensors
            data_source = ensure_data_source(name="Z-info", type="crawling script")
            sensors = ensure_zinfo_sensors(
                current_app.config.get("ZINFO_MAIN_SENSORS", {})
            )
            sensor_dict = {sensor.name: sensor for sensor in sensors}
            for zinfo_sensor_name in zinfo_sensor_names_received:
                if zinfo_sensor_name not in zinfo_sensor_mapping:
                    continue
                sensor_name = zinfo_sensor_mapping[zinfo_sensor_name]["fm_sensor_name"]
                if sensor_name not in sensor_dict:
                    current_app.logger.error(
                        f"No sensor set up for Z-info sensor name {zinfo_sensor_name} ..."
                    )
                    continue
                sensor = sensor_dict[sensor_name]
                df_sensor = df.loc[
                    (
                        df.index.get_level_values(zinfo_sensor_name_field)
                        == zinfo_sensor_name
                    )
                ].droplevel(zinfo_sensor_name_field)[zinfo_event_value_field]

                save_new_beliefs(df_sensor, data_source, sensor, now)

            # Save derived sensors
            sensors = ensure_zinfo_sensors(
                current_app.config.get("ZINFO_DERIVED_SENSORS", [])
            )
            for sensor in sensors:
                zinfo_sensor_name = sensor.zinfo_sensor_name
                if isinstance(zinfo_sensor_name, list):
                    mask = df.index.get_level_values(zinfo_sensor_name_field).isin(
                        zinfo_sensor_name
                    )
                    df_sensor = df.loc[mask]
                    df_sensor = apply_pandas_method_kwargs(
                        df_sensor, sensor.pandas_method_kwargs
                    )
                    df_sensor = df_sensor[zinfo_event_value_field]
                else:
                    mask = (
                        df.index.get_level_values(zinfo_sensor_name_field)
                        == zinfo_sensor_name
                    )
                    df_sensor = df.loc[mask]
                    df_sensor = apply_pandas_method_kwargs(
                        df_sensor, sensor.pandas_method_kwargs
                    )
                    df_sensor = df_sensor.droplevel(zinfo_sensor_name_field)[
                        zinfo_event_value_field
                    ]

                save_new_beliefs(df_sensor, data_source, sensor, now)


def localize_time_series(s: pd.Series, timezone: str) -> pd.Series:
    """Try inferring ambiguous local times, and otherwise skip them."""
    try:
        return (
            pd.to_datetime(s)
            .dt.tz_localize(timezone, ambiguous="infer")
            .dt.tz_convert(utc)
        )
    except AmbiguousTimeError as e:
        current_app.logger.error(f"Skipping ambiguous times due to a problem: {e} ...")
        return (
            pd.to_datetime(s)
            .dt.tz_localize(timezone, ambiguous="NaT")
            .dt.tz_convert(utc)
        )


def save_new_beliefs(df_sensor, data_source, sensor, belief_time) -> BeliefsDataFrame:
    # required by timely_beliefs, TODO: check if that still is the case, see https://github.com/SeitaBV/timely-beliefs/issues/64
    df_sensor.index.name = "event_start"
    df_sensor.name = "event_value"

    bdf = BeliefsDataFrame(
        df_sensor,
        source=data_source,
        sensor=sensor,
        belief_time=belief_time,
    )

    # Drop beliefs that haven't changed
    bdf = drop_unchanged_beliefs(bdf)

    # TODO: evaluate some traits of the data via FlexMeasures, see https://github.com/SeitaBV/flexmeasures-entsoe/issues/3
    save_to_db(bdf)


def apply_pandas_method_kwargs(
    df: pd.DataFrame, pandas_method_kwargs: List[Tuple[str, dict]]
) -> pd.DataFrame:
    """Convert the data frame using the given list.

    The conversion is defined using `pandas_method_kwargs`,
    which lists method/kwargs tuples that are called in the specified order.
    The example below converts from hourly meter readings in kWh to electricity demand in kW.

        pandas_method_kwargs=[
            ("diff", dict()),
            ("shift", dict(periods=-1)),
            ("head", dict(n=-1)),
        ],
    """
    for method, kwargs in pandas_method_kwargs:
        df = getattr(df, method)(**kwargs)
    return df


def ensure_zinfo_sensors(zinfo_sensors: List[dict]) -> List[Sensor]:
    """Set up sensors."""
    sensors = []
    for sensor_description in zinfo_sensors:
        generic_asset_name = sensor_description["generic_asset_name"]
        sensor_name = sensor_description["fm_sensor_name"]
        sensor = (
            Sensor.query.join(GenericAsset)
            .filter(
                Sensor.name == sensor_name,
                Sensor.generic_asset_id == GenericAsset.id,
                GenericAsset.name == generic_asset_name,
            )
            .one_or_none()
        )
        if sensor is None:
            current_app.logger.info(f"Adding sensor {sensor_name} ...")
            unit = sensor_description["unit"]
            timezone = sensor_description["timezone"]
            resolution = sensor_description["resolution"]
            generic_asset = GenericAsset.query.filter(
                GenericAsset.name == generic_asset_name
            ).one_or_none()
            if generic_asset is None:
                current_app.logger.error(
                    f"Missing generic asset {generic_asset_name}. First set it up with the FlexMeasures CLI."
                )
                continue
            sensor = Sensor(
                name=sensor_name,
                unit=unit,
                generic_asset=generic_asset,
                timezone=timezone,
                event_resolution=resolution,
            )
            db.session.add(sensor)
        sensor.zinfo_sensor_name = sensor_description["zinfo_sensor_name"]
        sensor.pandas_method_kwargs = sensor_description["pandas_method_kwargs"]
        sensors.append(sensor)
    db.session.commit()
    return sensors


def ensure_data_source(name: str, type: str) -> DataSource:
    data_source = DataSource.query.filter(DataSource.name == name).one_or_none()
    if not data_source:
        current_app.logger.info(f"Adding {name} data source ...")
        data_source = DataSource(name=name, type=type)
        db.session.add(data_source)
    return data_source
