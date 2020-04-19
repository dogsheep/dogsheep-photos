import hashlib
import pathlib

CONTENT_TYPES = {
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "png": "image/png",
    "gif": "image/gif",
    "heic": "image/heic",
}

HASH_BLOCK_SIZE = 1024 * 1024


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
