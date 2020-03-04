from setuptools import setup, find_packages

setup(
    name="pdpipewrench",
    version="0.8",
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
        "python-dotenv",
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
