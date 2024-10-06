#!/usr/bin/env python3
"""
Securities Analysis: Configurable Regression Analysis of Securities

Securities Analysis is an open source package for performing non linear
regression analysis on lists of mutual funds, exchange traded funds, and
stocks. The analysis is based on daily closing prices and is designed to be
executed after market close via a cron job or equivalent scheduler.  All
configuration parameters are specified in the options.json file located in the
installation folder under the data subdirectory.  The package utilizes NumPy,
Pandas, Requests, Scikit-learn, SciPy, and XlsxWriter.

Example
-------
To install Securities Analysis, simply execute the following command from the
download directory:

sudo python3 setup.py install

To execute Securities Analysis from the command line run the following:

securities-analysis

Notes
-----
Securities Analysis is distributed under the GNU Affero General Public License
v3 (https://www.gnu.org/licenses/agpl.html) as open source software with
attribution required.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.  See the GNU Affero General Public License v3
(https://www.gnu.org/licenses/agpl.html) for more details.

Copyright (C) 2024 John Sonsini.  All rights reserved.  Source code available
under the AGPLv3.

"""
import os
import setuptools
import site
import stat

result = setuptools.setup(
    name="securities-analysis",
    version="1.0",
    packages=setuptools.find_packages(),
    entry_points={
        "console_scripts": [
            "securities-analysis=securitiesanalysis.__main__:main"
        ]
    },
    python_requires=">3.6",
    install_requires=[
        "numpy>=1.19.2",
        "pandas>=1.1.5",
        "pandas_market_calendars>=4.0.1",
        "requests>=2.25.1",
        "scikit-learn>=0.23.2",
        "scipy>=1.5.2",
        "xlsxwriter>=1.3.7"
    ],
    include_package_data=True,
    author="John Sonsini",
    author_email="john.a.sonsini@gmail.com",
    description="Performs configurable regression analysis on securities",
    license="GNU AGPLv3",
    keywords="securities investments mutual fund stock etf analysis",
    url="https://github.com/jsonsini/securities-analysis",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Intended Audience :: Financial and Insurance Industry",
        "License :: OSI Approved :: GNU Affero General Public License v3",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.6",
        "Topic :: Office/Business :: Financial :: Investment",
        "Topic :: Scientific/Engineering :: Mathematics"
    ]
)
# Determining the path of the installation directory for the options file.
optionsPath = os.path.join(
    site.getsitepackages()[0],
    [f for f in os.listdir(site.getsitepackages()[0])
     if f.startswith(result.get_fullname())][0],
    "securitiesanalysis", "data", "options.json")
# Allow all users write access to options file so that package can be executed
# by any user.
os.chmod(optionsPath, os.stat(optionsPath)[stat.ST_MODE] | stat.S_IWOTH)
