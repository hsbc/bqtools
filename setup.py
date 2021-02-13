# -*- coding: utf-8 -*-
"""bqtools-json a module for managing interaction between json data and big query.

This module provides utility functions for big query and specificially treaing big query as json
document database.
Schemas can be defined in json and provides means to create such structures by reading or passing
json structures.

"""

import setuptools
from io import open

with open("README.md", "r", encoding='utf-8') as fh:
    long_description = fh.read()

setuptools.setup(
    name='bqtools-json',
    version='0.4.98',
    author="Mike Moore",
    author_email="z_z_zebra@yahoo.com",
    description="A Big Query json utility package",
    long_description_content_type="text/markdown",
    long_description=long_description,
    url="https://github.com/Mikemoore63/bqtools",
    packages=setuptools.find_packages(),
    test_suite='nose.collector',
    use_2to3=True,
    tests_require=['nose'],
    include_package_data=True,
    license="MIT",
    scripts=['bqtools/bqsync'],
    install_requires=[
        "jinja2<3.0",
        "google-cloud<1.0",
        "datetime<5.0",
        "google-cloud-bigquery<3.0",
        "google-cloud-storage<2.0",
        "google-cloud-logging<2.0",
        "absl-py<1.0",
        "boto<3.0",
        "deepdiff>=3.3.0,<6.0",
        'futures; python_version == "2.7"'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],

)
