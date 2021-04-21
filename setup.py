# -*- coding: utf-8 -*-

from setuptools import setup

with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='Equity Market Analysis',
    version='0.1.0',
    description='Data pipeline to ingest and process daily stock market data',
    long_description=readme,
    author='Ben Griffith',
    author_email='bengriffith@outlook.com',
    url='https://github.com/bengriffith/spring-capital',
    license=license
)

