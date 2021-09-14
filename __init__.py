__version__ = "0.2"

import os
import sys

import click
from flask import Blueprint, current_app
from flask.cli import with_appcontext
from flexmeasures.data.transactional import task_with_status_report
import requests

from .utils import get_access_token


HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

zinfo_bp = Blueprint("zinfo", __name__)
ZINFO_API_BASE_URL = "https://webservice.z-info.nl/WSR"
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
    res = requests.get(
        f"{ZINFO_API_BASE_URL}/zi_wsr.svc/JSON/NL.13/?spcid={zinfo_spcid}",
        headers={"Authorization": access_token},
    )
    response = res.json()
    values = response.get('waarden', [])
    current_app.logger.info(f"Got {len(values)} values...")
    print(f"Example: {values[0]}")
    # TODO: parse response
    if not dryrun:
        # TODO save
        pass
