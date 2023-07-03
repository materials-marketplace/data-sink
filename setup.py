from setuptools import find_packages, setup

from packageinfo import NAME, VERSION

# Read description
with open("README.md") as readme:
    README_TEXT = readme.read()

REQUIRES = ["uvicorn"]

setup(
    name=NAME,
    version=VERSION,
    description="API of a data-sink app for the MarketPlace",
    author_email="kiran.kumaraswamy@iwm.fraunhofer.de",
    url="",
    keywords=["Swagger", "API of a data-sink demo app for the MarketPlace"],
    packages=find_packages(),
    install_requires=REQUIRES,
    include_package_data=True,
    entry_points={
        "console_scripts": [
            "uvicorn_server=main:app",
            "start-server=uvicorn.main:run",
        ]
    },
    long_description=README_TEXT,
)
