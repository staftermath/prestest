#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pathlib import Path
from setuptools import setup, find_packages

here = Path(__file__).resolve().parent

def get_version():
    init_file = here / "prestest" / "__init__.py"
    with open(init_file, 'r') as f:
        for l in f.readlines():
            if l.startswith("__version__"):
                version = eval(l.strip().split(" ")[-1])
                return version

    raise RuntimeError("version not found")

def parse_requirements():
    requirement_file = here / "requirements.txt"
    requirements = []
    with open(requirement_file, 'r') as f:
        for l in f.readlines():
            l = l.strip()
            if not l.startswith("#"):
                requirements.append(l)

    return requirements

NAME = 'prestest'
DESCRIPTION = 'A framework for testing table creation in presto connected to hive'
URL = 'https://github.com/staftermath/prestest'
EMAIL = 'gwengww@gmail.com'
AUTHOR = 'Weiwen Gu'
REQUIRES_PYTHON = '>=3.6.0'
VERSION = get_version()

REQUIRED = parse_requirements()

try:
    with open(here / 'README.md', encoding='utf-8') as f:
        long_description = '\n' + f.read()
except FileNotFoundError:
    long_description = DESCRIPTION

about = {'__version__': VERSION}

setup(
    name=NAME,
    version=about['__version__'],
    description=DESCRIPTION,
    long_description=long_description,
    long_description_content_type='text/markdown',
    author=AUTHOR,
    author_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=find_packages(exclude=["tests", "*.tests", "*.tests.*", "tests.*"]),
    install_requires=REQUIRED,
    include_package_data=True,
)
