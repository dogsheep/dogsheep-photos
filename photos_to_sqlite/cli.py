import click
import sqlite_utils
import boto3
import json
import pathlib
from .utils import calculate_hash, image_paths, CONTENT_TYPES


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
    bucket = click.prompt("S3 bucket")
    access_key_id = click.prompt("Access key ID")
    secret_access_key = click.prompt("Secret access key")
    if pathlib.Path(auth).exists():
        auth_data = json.load(open(auth))
    else:
        auth_data = {}
    auth_data.update(
        {
            "photos_s3_bucket": bucket,
            "photos_s3_access_key_id": access_key_id,
            "photos_s3_secret_access_key": secret_access_key,
        }
    )
    open(auth, "w").write(json.dumps(auth_data, indent=4) + "\n")


@cli.command()
@click.argument(
    "db_path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.argument(
    "directories",
    nargs=-1,
    type=click.Path(file_okay=False, dir_okay=True, allow_dash=False),
)
@click.option(
    "-a",
    "--auth",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=True),
    default="auth.json",
    help="Path to auth.json token file",
)
@click.option(
    "--no-progress", is_flag=True, help="Don't show progress bar",
)
def upload(db_path, directories, auth, no_progress):
    "Upload photos from directories to S3"
    creds = json.load(open(auth))
    db = sqlite_utils.Database(db_path)
    client = boto3.client(
        "s3",
        aws_access_key_id=creds["photos_s3_access_key_id"],
        aws_secret_access_key=creds["photos_s3_secret_access_key"],
    )
    uploads = db.table("photos", pk="sha256")
    total_size = None
    bar = None
    if not no_progress:
        # Calculate total size first
        total_size = sum(p.stat().st_size for p in image_paths(directories))
        bar = click.progressbar(
            length=total_size,
            label="Uploading {total_size:.2f} GB".format(
                total_size=total_size / (1024 * 1024 * 1024)
            ),
            show_eta=True,
        )

    for path in image_paths(directories):
        resolved = path.resolve()
        sha256 = calculate_hash(resolved)
        ext = resolved.suffix.lstrip(".")
        size = path.stat().st_size
        uploads.upsert(
            {"sha256": sha256, "filepath": str(resolved), "ext": ext, "size": size}
        )
        keyname = "{}.{}".format(sha256, ext)
        client.upload_file(
            str(resolved),
            creds["photos_s3_bucket"],
            keyname,
            ExtraArgs={"ContentType": CONTENT_TYPES[ext]},
        )
        if bar:
            bar.update(size)
