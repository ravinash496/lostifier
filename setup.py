#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. currentmodule:: setup.py
.. moduleauthor:: Tom Weitzel

This file is used to create the package uploaded to PyPI/GemFury.
"""

import lostifier
from setuptools import setup, find_packages  # Always prefer setuptools over distutils
from codecs import open  # To use a consistent encoding
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the relevant file
with open(path.join(here, 'README'), encoding='utf-8') as f:
    long_description = f.read()

setup(
  name='lostifier',
  description="GeoComm's setup utilities for ECRF and LFV.",
  long_description="GeoComm's setup utilities for ECRF and LFV.",
  packages=find_packages(exclude=["docs", "*.tests", "*.tests.*", "tests.*", "tests"]),
  version=lostifier.__version__,
  install_requires=[
    'alabaster>=0.7.10',
    'appdirs>=1.4.3',
    'Babel>=2.4.0',
    'cement>=2.10.2',
    'colorama>=0.3.9',
    'docutils>=0.13.1',
    'GDAL>=2.1.0',
    'GeoAlchemy2>=0.4.0',
    'imagesize>=0.7.1',
    'Jinja2>=2.9.6',
    'MarkupSafe>=1.0',
    'packaging>=16.8',
    'psycopg2>=2.7.1',
    'Pygments>=2.2.0',
    'pyparsing>=2.2.0',
    'pytz>=2017.2',
    'requests>=2.14.2',
    'six>=1.10.0',
    'snowballstemmer>=1.2.1',
    'Sphinx>=1.6.1',
    'sphinxcontrib-websupport>=1.0.1',
    'SQLAlchemy>=1.1.9',
    'twine>=1.9.1',
    'typing>=3.6.1',
  ],
  python_requires=">=3.6.1",
  license='Proprietary',
  author='Tom Weitzel',
  author_email='tweitzel@geo-comm.com',
  classifiers=[
    # How mature is this project? Common values are
    #   3 - Alpha
    #   4 - Beta
    #   5 - Production/Stable
    'Development Status :: 4 - Beta',

    # Indicate who your project is intended for
    'Intended Audience :: Developers',
    'Topic :: Software Development :: Libraries',

    # Specify the Python versions you support here. In particular, ensure
    # that you indicate whether you support Python 2, Python 3 or both.
    'Programming Language :: Python :: 3.6',
  ],
  scripts=[
    'bin/load-gis',
    'bin/load-coverage',
  ]
)
