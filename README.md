# dogsheep-photos

[![PyPI](https://img.shields.io/pypi/v/dogsheep-photos.svg)](https://pypi.org/project/dogsheep-photos/)
[![Changelog](https://img.shields.io/github/v/release/dogsheep/dogsheep-photos?include_prereleases&label=changelog)](https://github.com/dogsheep/dogsheep-photos/releases)
[![CircleCI](https://circleci.com/gh/dogsheep/dogsheep-photos.svg?style=svg)](https://circleci.com/gh/dogsheep/dogsheep-photos)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/dogsheep/dogsheep-photos/blob/master/LICENSE)

Save details of your photos to a SQLite database and upload them to S3

## What these tools do

These tools are a work-in-progress mechanism for taking full ownership of your photos. The core idea is to help implement the following:

* Every photo you have taken lives in a single, private Amazon S3 bucket
* You have a single SQLite database file which stores metadata about those photos - potentially pulled from multiple different places. This may include EXIF data, Apple Photos, the results of running machine learning APIs against photos and much more besides.
* You can then use [Datasette](https://github.com/simonw/datasette) to explore your own photos.

I'm a heavy user of Apple Photos so the initial releases of this tool will have a bias towards that, but ideally I would like a subset of these tools to be useful to people no matter which core photo solution they are using.

## Installation

    $ pip install dogsheep-photos

## Authentication (if using S3)

If you want to use S3 to store your photos, you will need to first create S3 credentials for a new, dedicated bucket.

This is a big pain. Here's [how I did it](https://github.com/dogsheep/dogsheep-photos/issues/4).

Run this command and paste in your credentials. You will need three values: the name of your S3 bucket, your Access key ID and your Secret access key.

    $ dogsheep-photos s3-auth

This will create a file called `auth.json` in your current directory containing the required values. To save the file at a different path or filename, use the `--auth=myauth.json` option.

## Uploading photos

Run this command to upload every photo in a specific directory to your S3 bucket:

    $ dogsheep-photos upload photos.db \
        ~/Pictures/Photos\ Library.photoslibrary/original

The command will only upload photos that have not yet been uploaded, based on their sha256 hash.

`photos.db` will be created with an `uploads` table containing details of which files were uploaded.

To see what the command would do without uploading any files, use the `--dry-run` option.

The sha256 hash of the photo contents will be used as the name of the file in the bucket, with an extension matching the type of file. This is an implementation of the [Content addressable storage](https://en.wikipedia.org/wiki/Content-addressable_storage) pattern.

## Importing Apple Photos metadata

The `apple-photos` command imports metadata from your Apple Photos library.

    $ photo-to-sqlite apple-photos photos.db

Imported metadata includes places, people, albums, quality scores and machine learning labels for the photo contents.

## Creating a subset database

You can create a new, subset database of photos using the `create-subset` command.

This is useful for creating a shareable SQLite database that only contains metadata for a selected set of photos.

Since photo metadata contains latitude and longitude you may not want to share a database that includes photos taken at your home address.

`create-subset` takes three arguments: an existing database file created using the `apple-photos` command, the name of the new, shareable database file you would like to create and a SQL query that returns the `sha256` hash values of the photos you would like to include in that database.

For example, here's how to create a shareable database of just the photos that have been added to albums containing the word "Public":

    $ dogsheep-photos create-subset \
        photos.db \
        public.db \
        "select sha256 from apple_photos where albums like '%Public%'"
