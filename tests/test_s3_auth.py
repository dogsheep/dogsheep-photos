from click.testing import CliRunner
from dogsheep_photos.cli import cli
import json


def test_s3_auth():
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(cli, ["s3-auth"], input="bucket\nxxx\nyyy\nendpoint")
        assert 0 == result.exit_code
        data = json.load(open("auth.json"))
        assert {
            "photos_s3_bucket": "bucket",
            "photos_s3_access_key_id": "xxx",
            "photos_s3_secret_access_key": "yyy",
            "photos_s3_endpoint": "endpoint"
        } == data
