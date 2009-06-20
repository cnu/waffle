from distutils.core import setup
from distutils.command.install_data import install_data
from distutils.command.install import INSTALL_SCHEMES
import os
import sys

setup(
    name="waffle",
    version='0.1',
    url='http://github.com/bickfordb/waffle/',
    author='Brandon Bickford',
    author_email='bickfordb@gmail.com',
    description='A library to store schema-less data in relational databases',
    packages=['waffle'],
    classifiers = [
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)',
        'Programming Language :: Python',
        'Topic :: Database', 
        'Topic :: Software Development :: Libraries :: Python Modules',
    ]) 
