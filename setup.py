from setuptools import setup, find_packages

with open("README.md", "r") as f:
    page_description = f.read()

with open("requirements") as f:
    requirements = f.read().splitlines()

setup(
    name="WPR_advice",
    version="0.0.1",
    author="Milson Fortunato Neto",
    author_email="milsonfn@yahoo.com",
    description="Analysis with William's Percent Range",
    long_description=page_description,
    long_description_content_type="text/markdown",
    url="my_github_repository_project_link",
    packages=find_packages(),
    install_requires=requirements,
    python_requires='>=3.7',
)