import click
import concurrent.futures
import sqlite_utils
from sqlite_utils.db import OperationalError

try:
    import osxphotos
except ImportError:
    osxphotos = None
import sqlite3
import boto3
import json
import pathlib
from .utils import (
    calculate_hash,
    image_paths,
    CONTENT_TYPES,
    get_all_keys,
    osxphoto_to_row,
    osxphoto_to_score_row,
    to_uuid,
    s3_upload,
    hash_and_size_path,
)


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
@click.option(
    "--image-url-prefix",
    help="URL prefix of hosted images - suffix will be sha256.ext",
)
@click.option(
    "--image-url-suffix", help="URL suffix of hosted images, e.g. ?w=600", default=""
)
def apple_photos(db_path, library, image_url_prefix, image_url_suffix):
    "Import photo metadata from Apple Photos"
    if osxphotos is None:
        raise click.ClickException("Missing dependency osxphotos")
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

    db.conn.execute("ATTACH DATABASE '{}' AS attached".format(photosdb._tmp_db))
    if "apple_photos_scores" in db.table_names():
        db["apple_photos_scores"].drop()
    db.conn.execute(
        """
        CREATE TABLE apple_photos_scores(
        ZUUID TEXT,
        ZOVERALLAESTHETICSCORE REAL,
        ZCURATIONSCORE REAL,
        ZPROMOTIONSCORE REAL,
        ZHIGHLIGHTVISIBILITYSCORE REAL,
        ZBEHAVIORALSCORE REAL,
        ZFAILURESCORE REAL,
        ZHARMONIOUSCOLORSCORE REAL,
        ZIMMERSIVENESSSCORE REAL,
        ZINTERACTIONSCORE REAL,
        ZINTERESTINGSUBJECTSCORE REAL,
        ZINTRUSIVEOBJECTPRESENCESCORE REAL,
        ZLIVELYCOLORSCORE REAL,
        ZLOWLIGHT REAL,
        ZNOISESCORE REAL,
        ZPLEASANTCAMERATILTSCORE REAL,
        ZPLEASANTCOMPOSITIONSCORE REAL,
        ZPLEASANTLIGHTINGSCORE REAL,
        ZPLEASANTPATTERNSCORE REAL,
        ZPLEASANTPERSPECTIVESCORE REAL,
        ZPLEASANTPOSTPROCESSINGSCORE REAL,
        ZPLEASANTREFLECTIONSSCORE REAL,
        ZPLEASANTSYMMETRYSCORE REAL,
        ZSHARPLYFOCUSEDSUBJECTSCORE REAL,
        ZTASTEFULLYBLURREDSCORE REAL,
        ZWELLCHOSENSUBJECTSCORE REAL,
        ZWELLFRAMEDSUBJECTSCORE REAL,
        ZWELLTIMEDSHOTSCORE REAL
        );
        """
    )
    db["apple_photos_scores"].create_index(["ZUUID"])

    skipped = []

    with click.progressbar(photosdb.photos()) as photos:
        for photo in photos:
            rows = list(db["uploads"].rows_where("filepath=?", [photo.path]))
            if rows:
                sha256 = rows[0]["sha256"]
            else:
                if photo.ismissing:
                    print("Missing: {}".format(photo))
                    continue
                sha256 = calculate_hash(pathlib.Path(photo.path))
            photo_row = osxphoto_to_row(sha256, photo)
            db["apple_photos"].insert(
                photo_row, pk="uuid", replace=True, alter=True,
            )
            score_row = osxphoto_to_score_row(photo)
            db["apple_photos_scores"].insert(score_row, pk="ZUUID", replace=True, alter=True
            )
    # Ensure indexes
    for column in ("date", "sha256"):
        try:
            db["apple_photos"].create_index([column])
        except OperationalError:
            pass
    db.create_view(
        "photos_with_apple_metadata",
        """
    select
        apple_photos.rowid,{}
        apple_photos.uuid,
        apple_photos.date,
        apple_photos.albums,
        apple_photos.persons,
        uploads.ext,
        uploads.sha256,
        uploads.size,
        latitude,
        longitude,
        favorite,
        portrait,
        screenshot,
        slow_mo,
        time_lapse,
        hdr,
        selfie,
        panorama,
        place_city,
        place_state_province,
        place_country,
        apple_photos_scores.*
    from
        apple_photos
    join
        uploads on apple_photos.sha256 = uploads.sha256
    left join
        apple_photos_scores on apple_photos.uuid = apple_photos_scores.ZUUID
    order by
        apple_photos.date desc
    """.format(
            """
        json_object(
            'img_src',
            '{}' || uploads.sha256 || '.' || uploads.ext || '{}'
        ) as photo,""".format(
                image_url_prefix, image_url_suffix
            )
            if image_url_prefix
            else ""
        ),
        replace=True,
    )

    # Last step: import the labels
    labels_db_path = photosdb._dbfile_actual.parent / "search" / "psi.sqlite"
    if labels_db_path.exists():
        labels_db = sqlite3.connect(str(labels_db_path))
        if db["labels"].exists():
            db["labels"].drop()

        def all_labels():
            result = labels_db.execute(
                """
                select
                    ga.rowid,
                    assets.uuid_0,
                    assets.uuid_1,
                    groups.rowid as groupid,
                    groups.category,
                    groups.owning_groupid,
                    groups.content_string,
                    groups.normalized_string,
                    groups.lookup_identifier
                from
                    ga
                        join groups on groups.rowid = ga.groupid
                        join assets on ga.assetid = assets.rowid
                order by
                    ga.rowid
            """
            )
            cols = [c[0] for c in result.description]
            for row in result.fetchall():
                record = dict(zip(cols, row))
                id = record.pop("rowid")
                uuid = to_uuid(record.pop("uuid_0"), record.pop("uuid_1"))
                # Strip out the `\u0000` characters:
                for key in record:
                    if isinstance(record[key], str):
                        record[key] = record[key].replace("\x00", "")
                yield {"id": id, "uuid": uuid, **record}

        db["labels"].insert_all(all_labels(), pk="id", replace=True)
        db["labels"].create_index(["uuid"], if_not_exists=True)
        db["labels"].create_index(["normalized_string"], if_not_exists=True)


@cli.command(name="create-subset")
@click.argument(
    "db_path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False, exists=True),
)
@click.argument(
    "new_db_path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False, exists=False),
)
@click.argument("sql",)
def create_subset(db_path, new_db_path, sql):
    "Create a new subset database of photos with sha256 matching those returned by this SQL query"
    db = sqlite_utils.Database(db_path)
    new_db = sqlite_utils.Database(new_db_path)
    # Use the schema from the old database to create tables in the new database
    for result in db.conn.execute(
        "select sql from sqlite_master where sql is not null"
    ):
        new_db.conn.execute(result[0])
    # Figure out the photos to copy across
    sha256s = [r[0] for r in db.conn.execute(sql).fetchall()]
    # Copy across apple_photos, apple_photos_scores, uploads
    db.conn.execute("ATTACH DATABASE '{}' AS [{}]".format(str(new_db_path), "newdb"))
    # First apple_photos
    with db.conn:
        sql = """
            INSERT INTO
                newdb.apple_photos
            SELECT * FROM apple_photos WHERE sha256 in ({})
        """.format(
            ", ".join("'{}'".format(sha256) for sha256 in sha256s)
        )
        db.conn.execute(sql)
    # Now the other tables
    for sql in (
        """
            INSERT INTO
                newdb.apple_photos_scores
            SELECT * FROM apple_photos_scores WHERE ZUUID in (select uuid from newdb.apple_photos)
        """,
        """INSERT INTO
                newdb.labels
            SELECT * FROM labels WHERE uuid in (select uuid from newdb.apple_photos)""",
        """
            INSERT INTO
                newdb.uploads
            SELECT * FROM uploads WHERE sha256 in (select sha256 from newdb.apple_photos)
            """,
    ):
        with db.conn:
            db.conn.execute(sql)
