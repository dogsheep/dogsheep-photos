import click
import sqlite_utils
import boto3
import json
import pathlib
from .utils import calculate_hash

from PIL import Image
import PIL.ExifTags


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
def upload(db_path, directories, auth):
    "Upload photos from directories to S3"
    creds = json.load(open(auth))
    db = sqlite_utils.Database(db_path)
    client = boto3.client(
        "s3",
        aws_access_key_id=creds["photos_s3_access_key_id"],
        aws_secret_access_key=creds["photos_s3_secret_access_key"],
    )
    uploads = db.table("photos", pk="sha256")
    for directory in directories:
        path = pathlib.Path(directory)
        images = (
            p
            for p in path.glob("**/*")
            if p.suffix in [".jpg", ".jpeg", ".png", ".gif", ".heic"]
        )
        for imagepath in images:
            filepath = imagepath.resolve()
            sha256 = calculate_hash(filepath)
            size = filepath.stat().st_size
            ext = filepath.suffix.lstrip(".")
            uploads.upsert({"sha256": sha256, "filepath": str(filepath), "ext": ext, "size": size})
            print(filepath)
            keyname = "{}.{}".format(sha256, ext)

            image = Image.open(filepath)
            exif = {PIL.ExifTags.TAGS.get(k, str(k)): v for k, v in image.getexif().items()}
            uploads.update(sha256, exif, alter=True)
            # client.upload_file(
            #     str(filepath),
            #     "dogsheep-photos-simon",
            #     keyname,
            #     ExtraArgs={
            #         "ContentType": {
            #             "jpg": "image/jpeg",
            #             "jpeg": "image/jpeg",
            #             "png": "image/png",
            #             "gif": "image/gif",
            #             "heic": "image/heic",
            #         }[ext]
            #     },
            # )
            # print(
            #     " ... uploaded: {}".format(
            #         client.generate_presigned_url(
            #             "get_object",
            #             Params={"Bucket": "dogsheep-photos-simon", "Key": keyname,},
            #             ExpiresIn=600,
            #         )
            #     )
            # )
