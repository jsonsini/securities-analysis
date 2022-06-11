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

Routine Listings
----------------
history_update
    Retrieves daily closing price and metadata for securities.
securities_analysis
    Performs regression analysis and generates aggregations based on collected
    metadata.
regex_webscraper
    Customizable web scraping utility with regular expression based extraction.
utilities
    Helpful common functions and classes.

Notes
-----
Securities Analysis is distributed under the GNU Affero General Public License
v3 (https://www.gnu.org/licenses/agpl.html) as open source software with
attribution required.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.  See the GNU Affero General Public License v3
(https://www.gnu.org/licenses/agpl.html) for more details.

Copyright (C) 2022 John Sonsini.  All rights reserved.  Source code available
under the AGPLv3.

"""
from . import __main__ as main

__all__ = [
    "history_update",
    "securities_analysis",
    "regex_webscraper",
    "utilities"
]
