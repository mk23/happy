#!/usr/bin/env python2.7

from setuptools import setup
from lib import VERSION

if __name__ == '__main__':
    setup(
        author='Max Kalika',
        author_email='max.kalika+projects@gmail.com',
        url='https://github.com/mk23/happy',

        name='happy',
        version=VERSION,
        scripts=['happy'],
        packages=['happy'],
        package_dir={'happy': 'lib'},
        data_files=('/etc/happy', ['dataset.yaml.example']),
        license='LICENSE.txt',
        install_requires=['json', 'yaml', 'webhdfs', 'setuptools']
    )
