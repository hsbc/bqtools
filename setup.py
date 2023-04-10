# -*- coding: utf-8 -*-
"""bqtools-json a module for managing interaction between json data and big query.

This module provides utility functions for big query and specificially treaing big query as json
document database.
Schemas can be defined in json and provides means to create such structures by reading or passing
json structures.

"""

import setuptools
import re
from io import open

VERSIONFILE="bqtools/_version.py"
verstrline = open(VERSIONFILE, "rt").read()
VSRE = r"^__version__ = ['\"]([^'\"]*)['\"]"
mo = re.search(VSRE, verstrline, re.M)
if mo:
    verstr = mo.group(1)
else:
    raise RuntimeError("Unable to find version string in %s." % (VERSIONFILE,))

with open("README.md", "r", encoding='utf-8') as fh:
    long_description = fh.read()

setuptools.setup(
    name='bqtools-json',
    version=verstr,
    author="HSBC",
    author_email="opensource@hsbc.com",
    description="A Big Query json utility package",
    long_description_content_type="text/markdown",
    long_description=long_description,
    url="https://github.com/hsbc/bqtools",
    packages=setuptools.find_packages(),
    test_suite='nose.collector',
    tests_require=['nose',
                   "deepdiff>=3.3.0,<6.0"],
    include_package_data=True,
    scripts=['bqtools/bqsync'],
    install_requires=[
        "jinja2>=2.0,<4.0",
        "google-cloud<1.0",
        "google-cloud-bigquery>=2.6.0,<3.0",
        "google-cloud-storage>=1.0.0,<3.0",
        "google-cloud-logging>=3.0,<4.0",
        "absl-py~=1.0",
        "boto<3.0",
        "google-api-python-client>=2.0.0,<3.0",
        "grpcio~=1.0"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],

)
