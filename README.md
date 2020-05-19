# photos-to-sqlite

[![PyPI](https://img.shields.io/pypi/v/photos-to-sqlite.svg)](https://pypi.org/project/photos-to-sqlite/)
[![Changelog](https://img.shields.io/github/v/release/dogsheep/photos-to-sqlite?include_prereleases&label=changelog)](https://github.com/dogsheep/photos-to-sqlite/releases)
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

To see what the command would do without uploading any files, use the `--dry-run` option.

## Importing Apple Photos metadata

The `apple-photos` command can be run _after_ the `upload` command to import metadata from your Apple Photos library.

    $ photo-to-sqlite apple-photos photos.db

Imported metadata includes places, people, albums, quality scores and machine learning labels for the photo contents.

## Creating a subset database

You can create a new, subset database of photos using the `create-subset` command.

This is useful for creating a shareable SQLite database that only contains metadata for a selected set of photos.

Since photo metadata contains latitude and longitude you may not want to share a database that includes photos taken at your home address.

`create-subset` takes three arguments: an existing database file created using the `apple-photos` command, the name of the new, shareable database file you would like to create and a SQL query that returns the `sha256` hash values of the photos you would like to include in that database.

For example, here's how to create a shareable database of just the photos that have been added to albums containing the word "Public":

    $ photos-to-sqlite create-subset \
        photos.db \
        public.db \
        "select sha256 from apple_photos where albums like '%Public%'"
