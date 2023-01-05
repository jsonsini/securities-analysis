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

Notes
-----
Securities Analysis is distributed under the GNU Affero General Public License
v3 (https://www.gnu.org/licenses/agpl.html) as open source software with
attribution required.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.  See the GNU Affero General Public License v3
(https://www.gnu.org/licenses/agpl.html) for more details.

Copyright (C) 2023 John Sonsini.  All rights reserved.  Source code available
under the AGPLv3.

"""
import codecs
import datetime
import json
import os
import sys

import securitiesanalysis.history_update
import securitiesanalysis.securities_analysis
import securitiesanalysis.utilities


def load_options():
    """
    Load options.json file from installation directory.

    Ingests the options file and returns it as a dictionary along with the root
    of the installation directory.

    Returns
    -------
    options : dictionary
        Dictionary of configured options.
    root : str
        Folder containing all relevant subdirectories.

    """
    root = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(root, "data", "options.json"), "r",
                     "utf-8") as options_file:
        options = json.loads(options_file.read())
    return options, root


def is_trading_day(d, holidays):
    """
    Determine if passed in date is a trading day.

    Compares the given date with the list of holidays and if not a weekend day
    returns true.

    Parameters
    ----------
    d : date
        Date checked for trading day status.
    holidays : list
        List of known banking holidays from year 2000 to 2050.

    Returns
    -------
    t : boolean
        Determination of date being a trading day.

    """
    try:
        t = d.weekday() < 5 and str(d) not in holidays
    except:
        print("is trading day error for date %s %s"
              % (d,
                 securitiesanalysis.utilities.format_error(sys.exc_info())))
        return False
    else:
        return t


def main():
    """
    Executes history update and analysis modules.

    Load the options file, update the closing prices for all securities,
    perform the regression analysis, and save the results.

    """
    options, root = load_options()
    # Check to determine if current date is a trading day
    # if not is_trading_day(datetime.date.today(), options["holidays"]):
    #     print("%s is not a trading day, exiting" % str(datetime.date.today()))
    #     sys.exit(0)
    h = securitiesanalysis.history_update.HistoryUpdate(options)
    # Update the security histories
    h.execute()
    # Update the options file with the most recent split information
    with codecs.open(os.path.join(root, "data", "options.json"), "w",
                     "utf-8") as options_file:
        options_file.write(json.dumps(h.options, indent=4, sort_keys=True))
    s = securitiesanalysis.securities_analysis.SecuritiesAnalysis(
        h.root_path, h.options, h.data, h.message_list, h.log_date, h.log,
        h.err)
    # Perform the regression analysis and save the results
    s.execute()


if __name__ == "__main__":
    main()
