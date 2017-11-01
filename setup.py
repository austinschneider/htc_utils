'''
Austin Schneider 2017
PyCondor
Author: Austin Schneider
License: MIT
Code repository: https://github.com/hogenshpogen/htc_utils
'''

from setuptools import setup, find_packages
import htc_utils

VERSION = htc_utils.__version__

setup(
    name='htc_utils',
    version=VERSION,
    description='Python utility for HTCondor',
    url='https://github.com/hogenshpogen/htc_utils',
    author='Austin Schneider',
    author_email='aschneider@icecube.wisc.edu',
    license='MIT',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3'
    ],
    keywords='python condor htcondor high-throughput computing utility tool',
    packages=find_packages(),
    entry_points = {
    },
    package_data={'': ['LICENSE',
                       'README.md'],
                  'data': ['data/animals.txt',
                           'data/adjectives.txt',
                          ]
                 }
)
