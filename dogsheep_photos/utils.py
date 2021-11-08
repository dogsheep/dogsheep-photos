import boto3
import hashlib
import pathlib
import threading
import uuid
from datetime import timezone

CONTENT_TYPES = {
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "png": "image/png",
    "gif": "image/gif",
    "heic": "image/heic",
}

HASH_BLOCK_SIZE = 1024 * 1024

boto3_local = threading.local()


def calculate_hash(path):
    m = hashlib.sha256()
    with path.open("rb") as fp:
        while True:
            data = fp.read(HASH_BLOCK_SIZE)
            if not data:
                break
            m.update(data)
    return m.hexdigest()


def image_paths(directories):
    for directory in directories:
        path = pathlib.Path(directory)
        yield from (
            p
            for p in path.glob("**/*")
            if p.suffix in [".jpg", ".jpeg", ".png", ".gif", ".heic"]
        )


def get_all_keys(client, bucket):
    paginator = client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket):
        if "Contents" in page:
            for row in page["Contents"]:
                keys.append(row["Key"])
    return keys


def osxphoto_to_row(sha256, photo):
    row = {
        "sha256": sha256,
        "uuid": photo.uuid,
        "burst_uuid": photo._info["burstUUID"],
        "filename": photo.filename,
        "original_filename": photo.original_filename,
        "description": photo.description,
        "date": to_utc_isoformat(photo.date),
        "date_modified": to_utc_isoformat(photo.date_modified),
        "title": photo.title,
        "keywords": photo.keywords,
        "albums": photo.albums,
        "persons": photo.persons,
        "path": photo.path,
        "ismissing": photo.ismissing,
        "hasadjustments": photo.hasadjustments,
        "external_edit": photo.external_edit,
        "favorite": photo.favorite,
        "hidden": photo.hidden,
        "latitude": photo._latitude,
        "longitude": photo._longitude,
        "path_edited": photo.path_edited,
        "shared": photo.shared,
        "isphoto": photo.isphoto,
        "ismovie": photo.ismovie,
        "uti": photo.uti,
        "burst": photo.burst,
        "live_photo": photo.live_photo,
        "path_live_photo": photo.path_live_photo,
        "iscloudasset": photo.iscloudasset,
        "incloud": photo.incloud,
        "portrait": photo.portrait,
        "screenshot": photo.screenshot,
        "slow_mo": photo.slow_mo,
        "time_lapse": photo.time_lapse,
        "hdr": photo.hdr,
        "selfie": photo.selfie,
        "panorama": photo.panorama,
        "has_raw": photo.has_raw,
        "uti_raw": photo.uti_raw,
        "path_raw": photo.path_raw,
    }
    # Now add place keys
    place = photo.place
    if place is not None:
        for key, value in photo.place.address._asdict().items():
            row["place_{}".format(key)] = value
    return row


def to_utc_isoformat(dt):
    if not dt:
        return None
    fixed = dt.astimezone(timezone.utc).isoformat().split(".")[0]
    if not fixed.endswith("+00:00"):
        fixed += "+00:00"
    return fixed


def to_uuid(uuid_0, uuid_1):
    b = uuid_0.to_bytes(8, "little", signed=True) + uuid_1.to_bytes(
        8, "little", signed=True
    )
    return str(uuid.UUID(bytes=b)).upper()


def s3_upload(path, sha256, ext, creds):
    client = getattr(boto3_local, "client", None)
    if client is None:
        endpoint_url = creds["photos_s3_endpoint"] if "photos_s3_endpoint" in creds and creds["photos_s3_endpoint"] else None
        client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
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
