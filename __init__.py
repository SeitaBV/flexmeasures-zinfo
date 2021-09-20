__version__ = "0.2"

import os
import sys
from datetime import datetime
from pytz import utc
from typing import List

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
    "--dryrun/--no-dryrun",
    default=False,
    help="In Dry run, do not save the data to the db.",
)
@with_appcontext
@task_with_status_report
def import_sensor_data(dryrun: bool = False):
    """
    Import sensor data from Z-info.
    """
    access_token = get_access_token()
    zinfo_spcid = current_app.config.get("ZINFO_SPCID", None)
    if not zinfo_spcid:
        click.echo("ZINFO_SPCID setting is not given!")
        raise click.Abort
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
    df = df.iloc[::-1]  # switch order of values so that they run from past to present
    df[zinfo_event_value_field] = pd.to_numeric(df[zinfo_event_value_field])
    df[zinfo_event_end_field] = (
        pd.to_datetime(df[zinfo_event_end_field])
        .dt.tz_localize(ZINFO_TIMEZONE, ambiguous="infer")
        .dt.tz_convert(utc)
    )
    df = (
        df.set_index([zinfo_event_end_field, zinfo_sensor_name_field])
        .sort_index()[zinfo_event_value_field]
        .to_frame()
    )

    # Convert from meter data per Z-info sensor name (e.g. meterstanden) to time series data per FlexMeasures sensor
    zinfo_sensor_mapping: dict = current_app.config.get("ZINFO_SENSOR_MAPPING", {})
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
                f"Missing Z-info sensor name {zinfo_sensor_name} in your ZINFO_SENSOR_MAPPING config setting."
            )
            continue
        method_kwargs = zinfo_sensor_mapping[zinfo_sensor_name]["pandas_method_kwargs"]
        for method, kwargs in method_kwargs:
            df_sensor = getattr(df_sensor, method)(**kwargs)
        df_sensors.append(df_sensor)
    df = pd.concat(df_sensors, axis=0)

    if not dryrun:
        # Save
        data_source = ensure_data_source(name="Z-info", type="crawling script")
        sensors = ensure_zinfo_sensors()
        sensor_dict = {sensor.name: sensor for sensor in sensors}
        for zinfo_sensor_name in zinfo_sensor_names_received:
            if zinfo_sensor_name not in zinfo_sensor_mapping:
                continue
            sensor_name = zinfo_sensor_mapping[zinfo_sensor_name]["sensor_name"]
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

            # required by timely_beliefs, TODO: check if that still is the case, see https://github.com/SeitaBV/timely-beliefs/issues/64
            df_sensor.index.name = "event_start"
            df_sensor.name = "event_value"

            bdf = BeliefsDataFrame(
                df_sensor,
                source=data_source,
                sensor=sensor,
                belief_time=now,
            )

            # Drop beliefs that haven't changed
            bdf = drop_unchanged_beliefs(bdf)

            # TODO: evaluate some traits of the data via FlexMeasures, see https://github.com/SeitaBV/flexmeasures-entsoe/issues/3
            save_to_db(bdf)


def ensure_zinfo_sensors() -> List[Sensor]:
    zinfo_sensor_mapping = current_app.config.get("ZINFO_SENSOR_MAPPING", {})
    sensors = []
    for zinfo_sensor_name in zinfo_sensor_mapping:
        generic_asset_name = zinfo_sensor_mapping[zinfo_sensor_name][
            "generic_asset_name"
        ]
        sensor_name = zinfo_sensor_mapping[zinfo_sensor_name]["sensor_name"]
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
            unit = zinfo_sensor_mapping[zinfo_sensor_name]["unit"]
            timezone = zinfo_sensor_mapping[zinfo_sensor_name]["timezone"]
            resolution = zinfo_sensor_mapping[zinfo_sensor_name]["resolution"]
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
