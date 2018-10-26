from setuptools import setup, find_packages
setup(
    name="ghcn_tools",
    version="0.1",
    packages=find_packages(),

    install_requires=['pandas>=0.20', 'numpy>=1.0'],

    author="Adam Massmann",
    author_email="akm2203@columbia.edu",
    description="Tools for loading GHCN data"
)
