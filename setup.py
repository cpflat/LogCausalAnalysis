#!/usr/bin/env python

from __future__ import print_function

import os
import re
from setuptools import setup

try:
    from pypandoc import convert
    read_md = lambda f: convert(f, 'rst')
except ImportError:
    print('pandoc is not installed.')
    read_md = lambda f: open(f, 'r').read()

package_name = 'logcausality'
files = os.listdir(package_name)
re_df = re.compile(r"^.*\.sample$")
data_files = ["/".join((package_name, fn)) for fn in files if re_df.match(fn)]

setup(name='logcausality',
    version='0.0.3',
    description='',
    long_description=read_md('README.md'),
    author='Satoru Kobayashi',
    author_email='sat@nii.ac.jp',
    url='https://github.com/cpflat/LogCausalAnalysis/',
    install_requires=['scipy>=0.15.1', 'numpy>=1.9.2', 'networkx>=1.9.1',
        'py2-ipaddress>=3.4.1', 'gsq>=0.1.5', 'pcalg>=0.1.4',
        'subprocess32>=3.2.7'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2.7',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Software Development :: Libraries :: Python Modules'],
    license='The 3-Clause BSD License',
    
    packages=['logcausality'],
    package_data={'logcausality' : data_files},
    #data_files=[('config', [])]
    )
