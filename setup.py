"""
Setup file for the project.
"""

from setuptools import setup, find_packages

setup(
    name="BCG_Analysis",
    version="0.0.1",
    packages=find_packages(),
    install_requires=["pyspark==2.4.5", "pyyaml==5.3.1"],
    author="Deepanshu Tyagi",
    description="This is a case study analysis project for BCG.",
    url="https://github.com/Brillianttyagi/Case_Study.git",
)
