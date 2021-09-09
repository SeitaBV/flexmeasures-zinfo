import os
import sys

import click
from flask import Blueprint
from flask.cli import with_appcontext
from flexmeasures.data.transactional import task_with_status_report
import requests

from .utils import get_access_token


HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

__version__ = "0.1"

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
    Import sensor data
    """
    access_token = get_access_token()
    sensor_id = ""  # TODO: configure tthe sensors we need to read from
    res = requests.get(
        f"{ZINFO_API_BASE_URL}/zi_wsr.svc/JSON/NL.13/?spcid=hvcMwd_IR_wk",
        headers={"Authorization": access_token},
    )
    print(res.json())
    # TODO: parse response
    if not dryrun:
        # TODO save
        pass
