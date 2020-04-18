import hashlib

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
