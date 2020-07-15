#!/usr/bin/env python3
from os import path
from setuptools import setup

HERE = path.abspath(path.dirname(__file__))
with open(path.join(HERE, "README.md")) as f:
    README = f.read()

setup(
    name="genomicsqlite",
    version="0.0.1",
    url="https://github.com/mlin/GenomicSQLite",
    description="Genomics Extension for SQLite",
    long_description=README,
    long_description_content_type="text/markdown",
    author="Mike Lin",
    author_email="dna@mlin.net",
    py_modules=["genomicsqlite"],
    python_requires=">=3.6",
    entry_points={"console_scripts": ["genomicsqlite = genomicsqlite:_cli"]},
)
