# photos-to-sqlite

[![PyPI](https://img.shields.io/pypi/v/photos-to-sqlite.svg)](https://pypi.org/project/photos-to-sqlite/)
[![CircleCI](https://circleci.com/gh/dogsheep/photos-to-sqlite.svg?style=svg)](https://circleci.com/gh/dogsheep/photos-to-sqlite)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/dogsheep/photos-to-sqlite/blob/master/LICENSE)

Save details of your photos to a SQLite database and upload them to S3

## Installation

    $ pip install photos-to-sqlite

## Authentication

Create S3 credentials. This is a huge pain.

Run this command and paste in your credentials:

    $ photos-to-sqlite s3-auth

This will create a file called `auth.json` in your current directory containing the required values. To save the file at a different path or filename, use the `--auth=myauth.json` option.

## Uploading photos

Run this command to upload every photo in a specific directory to your S3 bucket:

    $ photos-to-sqlite upload photos.db ~/Desktop

The command will only upload photos that have not yet been uploaded, based on their sha256 hash.
