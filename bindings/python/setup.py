#!/usr/bin/env python3
from os import path
from setuptools import setup, find_packages
from version import get_version

HERE = path.abspath(path.dirname(__file__))
with open(path.join(HERE, "README.md")) as f:
    README = f.read()

setup(
    python_requires=">=3.6",
    name="genomicsqlite",
    version=get_version(),
    url="https://github.com/mlin/GenomicSQLite",
    description="Genomics Extension for SQLite",
    long_description=README,
    long_description_content_type="text/markdown",
    author="Mike Lin",
    author_email="dna@mlin.net",
    packages=find_packages(),
    package_data={"": ["genomicsqlite/libgenomicsqlite.so"]},
    include_package_data=True,
    entry_points={"console_scripts": ["genomicsqlite = genomicsqlite:_cli"]},
)
