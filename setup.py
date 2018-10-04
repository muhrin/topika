# -*- coding: utf-8 -*-

from __future__ import absolute_import
from setuptools import setup

__author__ = "Martin Uhrin"
__license__ = "Apache v2, see LICENSE file"
__contributors__ = "Sebastiaan Huber"

about = {}
with open('topika/version.py') as f:
    exec(f.read(), about)

setup(
    name="topika",
    version=about['__version__'],
    description='A python remote communications library',
    long_description=open('README.rst').read(),
    url='https://github.com/muhrin/topika.git',
    author='Martin Uhrin',
    author_email='martin.uhrin@gmail.com',
    license=__license__,
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Topic :: Internet',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Operating System :: MacOS',
        'Operating System :: POSIX',
        'Operating System :: Microsoft',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
    keywords='communication messaging rpc broadcast',
    # Abstract dependencies.  Concrete versions are listed in
    # requirements.txt
    # See https://caremad.io/2013/07/setup-vs-requirement/ for an explanation
    # of the difference and
    # http://blog.miguelgrinberg.com/post/the-package-dependency-blues
    # for a useful dicussion
    install_requires=['tornado>=4.0, <6.0', 'pika>=1.0.0b1', 'pyyaml>=3.0, <4.0', 'shortuuid', 'six', 'furl'],
    extras_require={
        'dev': ['pytest', 'pytest-cov', 'ipython', 'twine', 'pre-commit', 'yapf', 'prospector'],
        ':python_version<"3.5"': ['typing'],
        ':python_version<"3.4"': ['enum34', 'singledispatch'],
        ':python_version<"3.3"': ['mock'],
        ':python_version<"3.2"': ['backports.tempfile'],
    },
    packages=['topika'],
    test_suite='test')
