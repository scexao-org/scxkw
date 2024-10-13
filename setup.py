'''
    I want the repo to work with pip install -e .

    Can just grep the imports to keep track of the dependencies
'''

from setuptools import setup, find_packages
import glob

with open("README.md", 'r') as f:
    long_description = f.read()


script_list = glob.glob('./scripts/scxkw-*') + ['scripts/fitsheader']

setup(
        name = 'scxkw',
        version = '0.1',
        description = 'Keywork management package for SCExAO, between instrument, FITS headers, and Gen2',
        long_description = long_description,
        author = 'Vincent Deo',
        author_email = 'vdeo@naoj.org',
        url = "http://www.github.com/scexao-org/scxkw",
        packages = find_packages(),  # same as name
        install_requires = ['astropy', 'docopt', 'numpy', 'pandas', 'redis', 'sqlalchemy', 'tqdm', "numba", "click"],
        scripts = script_list
    )
