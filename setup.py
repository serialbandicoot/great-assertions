"""Great Assertions."""

import pathlib
from setuptools import setup, find_packages

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# This call to setup() does all the work
setup(
    name="great-assertions",
    version="0.0.32",
    description="Inspired by the library great-expectations",
    long_description=README,
    long_description_content_type="text/markdown",
    author="Sam Treweek",
    author_email="samtreweek@gmail.com",
    url="https://github.com/serialbandicoot/great-assertions",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.7",
    ],
    python_requires=">=3.6",  # Minimum version requirement of the package
    py_modules=["great_assertions"],  # Name of the python package
    package_dir={
        "": "great_assertions/src"
    },  # Directory of the source code of the package
    install_requires=[]
)
