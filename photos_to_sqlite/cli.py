import click
import concurrent.futures
import threading
import sqlite_utils
from sqlite_utils.db import OperationalError
import osxphotos
import boto3
import json
import pathlib
from .utils import (
    calculate_hash,
    image_paths,
    CONTENT_TYPES,
    get_all_keys,
    osxphoto_to_row,
)

boto3_local = threading.local()


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
@click.option(
    "--dry-run", is_flag=True, help="Don't upload, just show what would happen",
)
def upload(db_path, directories, auth, no_progress, dry_run):
    "Upload photos from directories to S3"
    creds = json.load(open(auth))
    db = sqlite_utils.Database(db_path)
    client = boto3.client(
        "s3",
        aws_access_key_id=creds["photos_s3_access_key_id"],
        aws_secret_access_key=creds["photos_s3_secret_access_key"],
    )
    click.echo("Fetching existing keys from S3...")
    existing_keys = {
        key.split(".")[0] for key in get_all_keys(client, creds["photos_s3_bucket"])
    }
    click.echo("Got {:,} existing keys".format(len(existing_keys)))
    # Now calculate sizes and hashes for files
    paths = list(image_paths(directories))
    hash_and_size = {}
    hash_bar = None
    if not no_progress:
        hash_bar = click.progressbar(paths, label="Calculating hashes")
    # hashlib docs say: 'For better multithreading performance,the Python GIL is
    # released for data larger than 2047 bytes at object creation or on update'
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_path = {
            executor.submit(hash_and_size_path, path.resolve()): path for path in paths
        }
        for future in concurrent.futures.as_completed(future_to_path):
            path, sha256, size = future.result()
            if hash_bar:
                hash_bar.update(1)
            hash_and_size[path] = (sha256, size)

    hashes = {v[0] for v in hash_and_size.values()}
    new_paths = [p for p in hash_and_size if hash_and_size[p][0] not in existing_keys]
    click.echo(
        "\n{:,} hashed files, {:,} are not yet in S3".format(
            len(hashes), len(new_paths)
        )
    )

    uploads = db.table("uploads", pk="sha256")
    total_size = None
    bar = None
    if dry_run or not no_progress:
        # Calculate total size first
        total_size = sum(hash_and_size[p][1] for p in new_paths)
        click.echo(
            "{verb} {num} files, {total_size:.2f} GB".format(
                verb="Would upload" if dry_run else "Uploading",
                num=len(new_paths),
                total_size=total_size / (1024 * 1024 * 1024),
            )
        )
        bar = click.progressbar(
            length=len(new_paths),
            label="Uploading {size:,} files".format(size=len(new_paths)),
            show_eta=True,
            show_pos=True,
        )

    if dry_run:
        return

    # Upload photos in a thread pool
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:

        future_to_path = {}
        for path in new_paths:
            ext = path.suffix.lstrip(".")
            sha256, size = hash_and_size[path]
            future = executor.submit(s3_upload, path, sha256, ext, creds)
            future_to_path[future] = path

        for future in concurrent.futures.as_completed(future_to_path):
            path = future.result()
            sha256, size = hash_and_size[path]
            ext = path.suffix.lstrip(".")
            uploads.upsert(
                {"sha256": sha256, "filepath": str(path), "ext": ext, "size": size}
            )
            if bar:
                bar.update(1)


@cli.command(name="apple-photos")
@click.argument(
    "db_path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.option(
    "--library",
    type=click.Path(file_okay=False, dir_okay=True, allow_dash=False),
    help="Location of Photos library to import",
)
def apple_photos(db_path, library):
    "Import photo metadata from Apple Photos"
    db = sqlite_utils.Database(db_path)
    # Ensure index
    try:
        db["uploads"].create_index(["filepath"])
    except OperationalError:
        pass

    if library:
        photosdb = osxphotos.PhotosDB(library)
    else:
        photosdb = osxphotos.PhotosDB()

    skipped = []

    with click.progressbar(photosdb.photos()) as photos:
        for photo in photos:
            rows = list(db["uploads"].rows_where("filepath=?", [photo.path]))
            if not rows:
                skipped.append(photo)
                continue
            assert len(rows) == 1
            sha256 = rows[0]["sha256"]
            photo_row = osxphoto_to_row(sha256, photo)
            db["apple_photos"].insert(
                photo_row,
                pk="uuid",
                replace=True,
                alter=True,
                foreign_keys=(("sha256", "uploads", "sha256"),),
            )
    print("Skipped {}".format(len(skipped)))
    # Ensure index
    try:
        db["apple_photos"].create_index(["date"])
    except OperationalError:
        pass
    db.create_view(
        "photos_with_apple_metadata",
        """
    select
        json_object(
            'img_src',
            'https://photos.simonwillison.net/i/' || uploads.sha256 || '.' || uploads.ext || '?w=600'
        ) as photo,
        apple_photos.date,
        apple_photos.albums,
        apple_photos.persons,
        latitude,
        longitude,
        favorite,
        portrait,
        screenshot,
        slow_mo,
        time_lapse,
        hdr,
        selfie,
        panorama
    from
        apple_photos
    join
        uploads on apple_photos.sha256 = uploads.sha256
    order by
        apple_photos.date desc
    """,
        replace=True,
    )


def s3_upload(path, sha256, ext, creds):
    client = getattr(boto3_local, "client", None)
    if client is None:
        client = boto3.client(
            "s3",
            aws_access_key_id=creds["photos_s3_access_key_id"],
            aws_secret_access_key=creds["photos_s3_secret_access_key"],
        )
        boto3_local.client = client
    keyname = "{}.{}".format(sha256, ext)
    client.upload_file(
        str(path),
        creds["photos_s3_bucket"],
        keyname,
        ExtraArgs={"ContentType": CONTENT_TYPES[ext]},
    )
    return path


def hash_and_size_path(path):
    size = path.stat().st_size
    sha256 = calculate_hash(path)
    return path, sha256, size
