from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="frameflow",
    version="1.5.0",
    author="Stephen Chen",
    author_email="your.email@example.com",
    description="Unified data transformation framework for Polars and PySpark",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/schen18/frameflow",
    packages=find_packages(),
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=[
        # No required dependencies - user chooses Polars or PySpark
    ],
    extras_require={
        "polars": [
            "polars>=0.19.0",
        ],
        "pyspark": [
            "pyspark>=3.3.0",
        ],
        "gcs": [
            "fsspec>=2023.1.0",
            "gcsfs>=2023.1.0",
        ],
        "all": [
            "polars>=0.19.0",
            "pyspark>=3.3.0",
            "fsspec>=2023.1.0",
            "gcsfs>=2023.1.0",
        ],
        "dev": [
            "pytest>=7.0.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
        ],
    },
    package_data={
        "frameflow": ["py.typed"],
    },
    include_package_data=True,
)