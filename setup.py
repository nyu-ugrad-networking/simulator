#!/usr/bin/env python3
from setuptools import setup, find_packages

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name="plab1_simulation",
    version='0.1',
    author="Aurojit Panda",
    author_email="apanada@cs.nyu.edu.edu",
    packages=find_packages(),
    install_requires=['networkx', 'pyyaml', 'matplotlib'],
    license="CRAPL",
)
