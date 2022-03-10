"""
Description:
    Script responsible for the project package configuration.
"""

from setuptools import setup
from setuptools import find_packages

setup(
    name="fire_information_etls",
    version="0.1.0",
    packages=find_packages(include=["fire_information_etl"]),
    install_requires=["pyspark", "apache-sedona"],
)
