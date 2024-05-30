#!/usr/bin/env python
from os import path

from setuptools import find_packages, setup

here = path.abspath(path.dirname(__file__))

setup(
    name="data-scraper-infrastructure",
    version="3.1",
    description="CDK code for deploying the serverless service",
    classifiers=[
        "Programming Language :: Python :: 3.10",
    ],
    packages=find_packages(exclude=["contrib", "docs", "tests"]),
    package_data={"": ["*.json"]},
    include_package_data=True,
    python_requires=">=3.10",
    install_requires=[],
)
