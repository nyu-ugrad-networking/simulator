#!/usr/bin/env python3
from setuptools import setup, find_packages

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name="plab2_simulation",
    version="0.2",
    author="Aurojit Panda",
    author_email="apanada@cs.nyu.edu.edu",
    packages=find_packages(),
    package_data={"plab1_sim": ["py.typed"]},
    install_requires=["networkx", "pyyaml", "matplotlib"],
    license="CRAPL",
)
