#!/usr/bin/env python3

import pathlib
from setuptools import setup, find_packages

import airflow_clickhouse_plugin.__version__ as about

here = pathlib.Path(__file__).parent.absolute()
with open(here / 'README.md') as readme_file:
    long_description = readme_file.read()
with open(here / 'requirements.txt') as requirements_file:
    install_requires = [
        requirement_line.rstrip('\n')
        for requirement_line in requirements_file
    ]

setup(
    name=about.__title__,
    description=about.__description__,
    long_description=long_description,
    long_description_content_type='text/markdown',
    version=about.__version__,
    author=about.__author__,
    author_email=about.__author_email__,
    url=about.__url__,
    packages=find_packages(exclude=('tests', 'tests.*')),
    include_package_data=True,
    python_requires=">=3.7.*",
    install_requires=install_requires,
    extras_require={
        'pandas': ['apache-airflow[pandas]>=2.0.0,<2.5.0'],
    },
    license=about.__license__,
    zip_safe=False,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Plugins',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.7',
    ],
    keywords=['clickhouse', 'airflow'],
)
