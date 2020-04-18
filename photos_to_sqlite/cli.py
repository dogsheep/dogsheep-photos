import click
import sqlite_utils
import json
import pathlib


@click.group()
@click.version_option()
def cli():
    "Save details of your photos to a SQLite database and upload them to S3"


@cli.command(name="s3-auth")
@click.option(
    "-a",
    "--auth",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    default="auth.json",
    help="Path to save tokens to, defaults to auth.json",
)
def s3_auth(auth):
    "Save S3 credentials to a JSON file"
    click.echo("Create S3 credentials and paste them here:")
    click.echo()
    access_key_id = click.prompt("Access key ID")
    secret_access_key = click.prompt("Secret access key")
    if pathlib.Path(auth).exists():
        auth_data = json.load(open(auth))
    else:
        auth_data = {}
    auth_data.update(
        {
            "photos_s3_access_key_id": access_key_id,
            "photos_s3_secret_access_key": secret_access_key,
        }
    )
    open(auth, "w").write(json.dumps(auth_data, indent=4) + "\n")
