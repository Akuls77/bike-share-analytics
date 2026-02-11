from setuptools import find_packages, setup

setup(
    name="bike_share_dagster",
    packages=find_packages(exclude=["bike_share_dagster_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
