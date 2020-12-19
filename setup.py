from setuptools import setup
import os

VERSION = "0.4.1"


def get_long_description():
    with open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.md"),
        encoding="utf8",
    ) as fp:
        return fp.read()


setup(
    name="dogsheep-photos",
    description="Save details of your photos to a SQLite database and upload them to S3",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="Simon Willison",
    url="https://github.com/dogsheep/dogsheep-photos",
    license="Apache License, Version 2.0",
    version=VERSION,
    packages=["dogsheep_photos"],
    entry_points="""
        [console_scripts]
        dogsheep-photos=dogsheep_photos.cli:cli
    """,
    install_requires=[
        "sqlite-utils>=2.7",
        "boto3>=1.12.41",
        "osxphotos>=0.38.6 ; sys_platform=='darwin'",
    ],
    extras_require={"test": ["pytest"]},
    tests_require=["dogsheep-photos[test]"],
)
