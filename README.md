HAPPY
=====

Hadoop Pull via Python application synchronizes an HDFS directory to a local filesystem.  It has support for automatically extracting archives and processing datasets.

Table of Contents
-----------------
* [Prerequisites](#prerequisites)
* [Installation](#installation)
* [Administration](#administration)
* [Configuration](#configuration)
* [License](#license)

Prerequisites
-------------

* Python 2.7+
* Python [yaml](http://pyyaml.org) module
* Python [webhdfs](https://github.com/mk23/webhdfs) module

Installation
------------

Install HaPPy as a Debian package by building a `deb`:

    dpkg-buildpackage
    # or
    pdebuild

Install HaPPy using the standard `setuptools` script:

    python setup.py install

License
-------
[MIT](http://mk23.mit-license.org/2017/license.html)
