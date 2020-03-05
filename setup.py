from setuptools import setup, find_packages
from os import path

# read the contents of your README file
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name="pdpipewrench",
    version="1.1",
    description="YAML-configurable Pandas pipelines.",
    long_description=long_description,
    long_description_content_type='text/markdown',
    url="https://github.com/blakeNaccarato/pdpipewrench",
    author="Blake Naccarato",
    package_dir={"": "src"},
    packages=find_packages(where="src"),  # Required
    python_requires=">=3.7",
    install_requires=[
        "pandas",
        "pdpipe",
        "engarde",
        "confuse",
    ],
    extras_require={  # pip install -e .[dev]
        "dev": [
            # data science
            "numpy",
            "scipy",
            # document
            "doc8",
            # experiment
            "jupyter",
            # format
            "black",
            # lint
            "flake8",
            "mypy",
            "pylint",
            # matplotlib w/ backend
            "matplotlib",
            "PyQt5",
            # refactor
            "rope",
        ],
    },
)
