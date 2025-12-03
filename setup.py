from setuptools import setup, find_packages

setup(
    name="DaggyD",
    version="0.1.1",
    packages=find_packages(),
    install_requires=["DaggyD.daggyd"],
    python_requires=">=3.8",
    include_package_data=True,  # Force additional files into the package
)
