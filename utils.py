import os
import yaml
from datetime import datetime, timedelta

from flask import current_app
import requests
import pickle
import click


def get_access_token() -> str:
    """
    Get the auth token or re-use a previously retrieved one.
    TODO: Use refresh_token to get a new one when the current one is expired?
          (instead of using password)
    """
    access_storage = ".zinfo_access.pkl"
    now = datetime.now()
    if os.path.exists(access_storage):
        with open(access_storage, "rb") as f:
            access_token, valid_until = pickle.load(f)
        if now < valid_until:
            print("Re-using earlier Z-info access token ...")
            return access_token
    print("Getting a fresh Z-info access token ...")
    zinfo_username = current_app.config.get("ZINFO_USERNAME", None)
    if not zinfo_username:
        click.echo("ZINFO_USERNAME setting is not given!")
        raise click.Abort
    zinfo_password = current_app.config.get("ZINFO_PASSWORD", None)
    if not zinfo_password:
        click.echo("ZINFO_USERNAME setting is not given!")
        raise click.Abort
    res = requests.post(
        "https://webservice.z-info.nl/WSR/zi_wsr.svc/token",
        headers={"content-type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "password",
            "username": zinfo_username,
            "password": zinfo_password,
        },
    )
    assert res.status_code == 200, f"status code is {res.status_code} ({res.text})"
    response = res.json()
    assert response["token_type"] == "bearer"

    access_token = response["access_token"]

    valid_until = now + timedelta(seconds=int(response["expires_in"]))
    with open(access_storage, "wb") as f:
        pickle.dump([access_token, valid_until], f)

    return access_token


def log_notifications(response):
    """Log notifications, if any."""
    notifications = response.get("meldingen", [])
    if notifications:
        current_app.logger.info(
            f"Got {len(notifications)} notifications from Z-info:\n{yaml.dump(notifications, indent=4)}"
        )
