from setuptools import setup, find_packages

setup(
    name="DaggyD",
    version="0.1.0",
    packages=find_packages(),
    install_requires=["DaggyD.daggyd"],
    python_requires=">=3.12",
)
