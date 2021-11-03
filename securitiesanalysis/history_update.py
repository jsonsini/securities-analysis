#!/usr/bin/env python3
"""
Updates security price history files and collects related metadata.

This module generates the list of securities by type, downloads the daily
closing prices for all securities, downloads the relevant metadata for each
ticker symbol, updates the corresponding history files, and corrects histories
for any new splits found.

The HistoryUpdate class is the main class for processing daily updates with the
exception of two functions, get_metadata and scrape_EOD, that due to pickling
limitations of the concurrent module are not implemented as instance methods.

Notes
-----
Securities Analysis is distributed under the GNU Affero General Public License
v3 (https://www.gnu.org/licenses/agpl.html) as open source software with
attribution required.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.  See the GNU Affero General Public License v3
(https://www.gnu.org/licenses/agpl.html) for more details.

Copyright (C) 2021 John Sonsini.  All rights reserved.  Source code available
under the AGPLv3.

"""
import codecs
import datetime
import multiprocessing
import os
import re
import string
import sys

import pandas

import securitiesanalysis.regex_webscraper
import securitiesanalysis.utilities


class HistoryUpdate(object):
    """
    Retrieves daily closing price and metadata for securities.

    Creates any needed directories before configuring the regular expression
    based web scraper, then downloads all daily closing prices and related
    metadata for the extracted lists of ticker symbols and updates the history
    files accounting for any new split information to make appropriate
    adjustments.

    """

    _scraper = securitiesanalysis.regex_webscraper.RegexWebScraper([".*"], 0,
                                                                   0, 0)
    """obj: Static regular expression based web scraping utility."""

    def __init__(self, options):
        """
        Prepare all needed instance variables for execution.

        Sets up all paths, logging, the message list, applied split set, and
        holiday list.

        Parameters
        ----------
        options : dictionary
            Dictionary of configured options.

        """
        self.__options = options
        """dictionary: Dictionary of configured options."""
        self.__root_path = self.__options["root_path"]
        """str: Folder containing all relevant subdirectories."""
        self.__log_path = os.path.join(self.__root_path, "logs")
        """str: Folder containing log message files."""
        self.__error_path = os.path.join(self.__root_path, "errors")
        """str: Folder containing error message files."""
        self.__history_path = os.path.join(self.__root_path, "history")
        """str: Folder containing security history files."""
        self.__report_path = os.path.join(self.__root_path, "reports")
        """str: Folder containing all output directories and files."""
        self.__log_date = datetime.date.today()
        """date: Day of execution."""
        self.__log = securitiesanalysis.utilities.Logger(
            os.path.join(self.__log_path, "%s.log" % self.__log_date))
        """obj: Logging utility for standard operating messages."""
        self.__err = securitiesanalysis.utilities.Logger(
            os.path.join(self.__error_path, "%s.log" % self.__log_date))
        """obj: Logging utility for unexpected messages."""
        self.__message_list = ["market summary for %s" % str(self.__log_date),
                               ""]
        """obj: List of messages to be included in body of summary email."""
        self.__applied_split_set = set(self.__options["applied_split_set"])
        """set: Collection of previous splits applied to history files."""
        self.__data = None
        """obj: All closing prices and metadata for each symbol."""

    def __initialize_directories__(self):
        """
        Ensures folders needed for processing are available.

        Checks to determine if all necessary directories have been previously
        generated and creates them if needed.

        Returns
        -------
        boolean
            Indicates whether all directories were created successfully.

        """
        try:
            if not os.path.exists(self.__root_path):
                os.makedirs(self.__root_path)
            if not os.path.exists(self.__log_path):
                os.makedirs(self.__log_path)
            if not os.path.exists(self.__error_path):
                os.makedirs(self.__error_path)
            if not os.path.exists(self.__history_path):
                os.makedirs(self.__history_path)
            if not os.path.exists(self.__report_path):
                os.makedirs(self.__report_path)
            if not os.path.exists(os.path.join(self.__report_path, "data")):
                os.makedirs(os.path.join(self.__report_path, "data"))
            if not os.path.exists(os.path.join(self.__report_path, "summary")):
                os.makedirs(os.path.join(self.__report_path, "summary"))
        except:
            print("initialize directories error %s"
                  % securitiesanalysis.utilities.format_error(sys.exc_info()))
            return False
        else:
            return True

    def __configure_scraper__(self):
        """
        Sets up regular expression based web scraping utility.

        Configures the reusable web scraper with default parameters common to
        all subsequent usage.

        """
        type(self)._scraper.timeout = self.__options["timeout_period"]
        type(self)._scraper.delay_time = self.__options["delay_time"]
        type(self)._scraper.max_retries = self.__options["max_retry_count"]

    def __get_fund_total_assets_category(self, symbol):
        """
        Retrieves assets and category from configured online source.

        Downloads and extracts asset and category information for passed ticker
        symbol, transforming asset text to an integer value and finding the
        mapped category value.  If the category value is not in the mapping
        then a warning message is included in the email body with the results
        so that the options file can be manually updated.

        Parameters
        ----------
        symbol : str
            Ticker symbol representing security.

        Returns
        -------
        a : int
            Net Assets for Mutual Fund.
        c : str
            Sector or grouping ticker symbol belongs to, standard mapping is
            defined in the options file such that any unmapped categories
            encountered will be added to a message list and included in the
            subsequent email for the summary.

        """
        p = multiprocessing.current_process().name
        self.__log.log(
            "get fund total assets and category for %s %s" % (symbol, p))
        try:
            matches = type(self)._scraper.scrape(
                self.__options["fund_total_assets_category_prefix_URL"]
                % symbol)
            a, c = matches[0], matches[1]
            # Multiply by the correct order of magnitude based on the trailing
            # character
            if a[-1] == "K":
                a = int(1000 * float(a[:-1]))
            elif a[-1] == "M":
                a = int(1000000 * float(a[:-1]))
            elif a[-1] == "B":
                a = int(1000000000 * float(a[:-1]))
            elif a[-1] == "T":
                a = int(1000000000000 * float(a[:-1]))
            else:
                a = -1
        except:
            a = -1
        try:
            c = "UNKNOWN" if not c or c == "--" else c
            # To ensure later aggregations include all member securities map
            # the collected category to a standardized set in the configuration
            # file
            if c in self.__options["category_mapping"]:
                c = self.__options["category_mapping"][c]
            else:
                self.__log.log("warning - unmapped category of %s for %s"
                               % (c, symbol))
                self.__message_list.append(
                    "warning - unmapped category of %s for %s" % (c, symbol))
        except:
            c = "UNKNOWN"
        self.__log.log("got fund total assets and category for %s %s %s %s"
                       % (symbol, a, c, p))
        return a, c

    def __get_fund_family(self, symbol):
        """
        Retrieves family from configured online source.

        Downloads and extracts investment firm information for passed ticker
        symbol.

        Parameters
        ----------
        symbol : str
            Ticker symbol representing security.

        Returns
        -------
        f : str
            Investment firm managing the fund.

        """
        p = multiprocessing.current_process().name
        self.__log.log("get fund family for %s %s" % (symbol, p))
        try:
            matches = type(self)._scraper.scrape(
                "%s%s" % (self.__options["fund_family_prefix_URL"], symbol))
            if matches[0]:
                f = re.split("</span><span>", matches[0])[0]
                f = "UNKNOWN" if f == "--" else f
            else:
                f = "UNKNOWN"
        except:
            f = "UNKNOWN"
        self.__log.log("got fund family for %s %s %s" % (symbol, f, p))
        return f

    def __get_etf_total_assets_family_category(self, symbol):
        """
        Retrieves assets, family, and category from configured online source.

        Downloads and extracts asset, family, and category information for
        passed ticker symbol, finding the mapped category value.  If the
        category value is not in the mapping then a warning message is included
        in the email body with the results so that the options file can be
        manually updated.

        Parameters
        ----------
        symbol : str
            Ticker symbol representing security.

        Returns
        -------
        a : str
            Net Assets for the fund.
        f : str
            Investment firm managing the fund.
        c : str
            Sector or grouping ticker symbol belongs to, standard mapping is
            defined in the options file such that any unmapped categories
            encountered will be added to a message list and included in the
            subsequent email for the summary.

        """
        p = multiprocessing.current_process().name
        self.__log.log(
            "get etf assets, family, and category for %s %s " % (symbol, p))
        try:
            matches = type(self)._scraper.scrape(
                "%s%s" % (self.__options["etf_prefix_URL"], symbol))
            a, f, c = matches[0], matches[1], matches[2]
            # Multiply by the correct order of magnitude based on the trailing
            # character
            if a[-1] == "K":
                a = int(1000 * float(a[:-1]))
            elif a[-1] == "M":
                a = int(1000000 * float(a[:-1]))
            elif a[-1] == "B":
                a = int(1000000000 * float(a[:-1]))
            elif a[-1] == "T":
                a = int(1000000000000 * float(a[:-1]))
            else:
                a = -1
            f = "UNKNOWN" if not f or not f.strip() else f
            c = "UNKNOWN" if not c or not c.strip() or c == "--" else c
            # To ensure later aggregations include all member securities map
            # the collected category to a standardized set in the configuration
            # file
            if c in self.__options["category_mapping"]:
                c = self.__options["category_mapping"][c]
            else:
                self.__log.log("warning - unmapped category of %s for %s"
                               % (c, symbol))
                self.__message_list.append(
                    "warning - unmapped category of %s for %s" % (c, symbol))
        except:
            a = -1
            f = "UNKNOWN"
            c = "UNKNOWN"
        self.__log.log(
            "got etf assets, family, and category for %s %s %s %s %s"
            % (symbol, a, f, c, p))
        return a, f, c

    def __get_stock_total_assets_category(self, symbol):
        """
        Retrieves assets and category from configured online source.

        Downloads and extracts assets and category information for passed
        ticker symbol, finding the mapped category value.  If the category
        value is not in the mapping then a warning message is included in the
        email body with the results so that the options file can be manually
        updated.

        Parameters
        ----------
        symbol : str
            Ticker symbol representing security.

        Returns
        -------
        a : str
            Market capitalization for the stock.
        c : str
            Sector or grouping ticker symbol belongs to, standard mapping is
            defined in the options file such that any unmapped categories
            encountered will be added to a message list and included in the
            subsequent email for the summary.

        """
        p = multiprocessing.current_process().name
        self.__log.log("get stock assets and category for %s %s" % (symbol, p))
        try:
            matches = type(self)._scraper.scrape(
                "%s%s" % (self.__options["stock_prefix_URL"], symbol))
            a, c = matches[0], matches[1]
            if a[-1] == "K":
                a = int(1000 * float(a[:-1]))
            elif a[-1] == "M":
                a = int(1000000 * float(a[:-1]))
            elif a[-1] == "B":
                a = int(1000000000 * float(a[:-1]))
            elif a[-1] == "T":
                a = int(1000000000000 * float(a[:-1]))
            else:
                a = -1
        except:
            a = -1
        try:
            c = "UNKNOWN" if not c or c == "--" else c
            # To ensure later aggregations include all member securities map
            # the collected category to a standardized set in the configuration
            # file
            if c in self.__options["category_mapping"]:
                c = self.__options["category_mapping"][c]
            else:
                self.__log.log("warning - unmapped category of %s for %s"
                               % (c, symbol))
                self.__message_list.append(
                    "warning - unmapped category of %s for %s" % (c, symbol))
        except:
            c = "UNKNOWN"
        self.__log.log("got stock assets and category for %s %s %s %s"
                       % (symbol, a, c, p))
        return a, c

    def get_metadata(self, symbol_tuple):
        """
        Retrieves daily closing price and metadata for each security.

        Based on ticker symbol and security type, this function collects the
        net assets or market capitalization, cap, category, and family.

        Parameters
        ----------
        symbol_tuple : tuple
            Tuple containing strings for ticket symbol and security type.

        Returns
        -------
        assets : float
            Net assets for mutual funds and exchange traded funds, market
            capitalization for stocks.
        cap : str
            "large", "mid", or "small" defined by greater than $10B, between $2B
            and $10B, and less than $2B.
        category : str
            Sector or grouping ticker symbol belongs to.
        family : str
            Only for mutual funds and exchange traded funds, the investment firm
            managing the fund.

        """
        p = multiprocessing.current_process().name
        symbol, security_type = symbol_tuple
        self.__log.log(
            "get metadata for %s %s %s" % (symbol, security_type, p))
        # Provide default values in case security metadata fields are not
        # available
        assets, cap, category, family = -1, "UNKNOWN", "UNKNOWN", "UNKNOWN"
        # Based on security type collect the appropriate metadata
        if security_type == "fund":
            # Update the regular expression patterns and attributes to collect
            self._scraper.pattern_list = [
                self.__options["fund_assets_pattern"],
                self.__options["fund_category_pattern"]]
            self._scraper.findall = False
            self._scraper.groups = (1,)
            assets, category = self.__get_fund_total_assets_category(
                symbol)
            cap = securitiesanalysis.utilities.get_cap(assets)
            self._scraper.pattern_list = [
                self.__options["fund_family_pattern"]]
            self._scraper.groups = None
            family = self.__get_fund_family(symbol)
        elif security_type == "etf":
            # Update the regular expression patterns and attributes to collect
            self._scraper.pattern_list = [
                self.__options["etf_assets_pattern"],
                self.__options["etf_family_pattern"],
                self.__options["etf_category_pattern"]]
            self._scraper.findall = False
            self._scraper.groups = (1,)
            assets, family, category = \
                self.__get_etf_total_assets_family_category(symbol)
            cap = securitiesanalysis.utilities.get_cap(assets)
        elif security_type == "stock":
            # Update the regular expression patterns and attributes to collect
            self._scraper.pattern_list = [
                self.__options["stock_assets_pattern"],
                self.__options["stock_category_pattern"]]
            self._scraper.findall = False
            self._scraper.groups = (1,)
            assets, category = self.__get_stock_total_assets_category(symbol)
            cap = securitiesanalysis.utilities.get_cap(assets)
            # Stocks do not belong to any family and should not be grouped with
            # mutual funds and exchange traded funds marked "UNKNOWN"
            family = None
        else:
            self.__err.log(
                "get metadata unknown type %s for %s %s" % (security_type,
                                                            symbol, p))
        self.__log.log(
            "got metadata for %s %s %s %s %s %s %s" % (symbol, security_type,
                                                       assets, cap, category,
                                                       family, p))
        return assets, cap, category, family

    def scrape_eod(self, eod_url, initial_type):
        """
        Retrieves daily closing price and metadata for list extracted from URL.

        Creates process pool to collect daily closing prices and metadata for all
        extracted ticker symbols from the provided URL.  Should an exception be
        thrown during processing an empty dataframe with the correct columns will
        be returned.

        Parameters
        ----------
        eod_url : str
            URL to generate list of securities from.
        initial_type : str
            "fund", "etf", or "stock" to indicate security type.

        Returns
        -------
        obj
            Columnar format containing ticker symbols, daily closing prices,
            titles, and all metadata for every security scraped from the URL.

        """
        p = multiprocessing.current_process().name
        try:
            self.__log.log("scrape eod %s %s" % (eod_url, p))
            # Collect data matching the configured regular expressions from the
            # passed in web page
            data = self._scraper.scrape(eod_url)
            symbols = [d[0] for d in data[0]]
            titles = [d[1] for d in data[0]]
            # Remove commas from prices so data can be treated as numeric
            prices = [d[2].replace(",", "") for d in data[0]]
            security_types = [initial_type for d in data[0]]
            # Create a concurrent process pool to execute the scraping in
            # parallel
            with securitiesanalysis.utilities.NonDaemonicPool(
                    processes=self.__options["metadata_pool_size"]) as pool:
                metadata = pool.starmap(
                    self.get_metadata,
                    [(s,) for s in zip(symbols, security_types)])
            # Separate the metadata by field for insertion into a dataframe
            assets = [m[0] for m in metadata]
            cap = [m[1] for m in metadata]
            categories = [m[2] for m in metadata]
            families = [m[3] for m in metadata]
            self.__log.log("scraped eod %s %s" % (eod_url, p))
        except:
            self.__err.log(
                "scrape eod generic error for %s %s %s"
                % (eod_url,
                   securitiesanalysis.utilities.format_error(sys.exc_info()),
                   p))
            return pandas.DataFrame(index=list(),
                                    data={"type": list(), "title": list(),
                                          "price": list(), "assets": list(),
                                          "cap": list(), "category": list(),
                                          "family": list()})
        else:
            return pandas.DataFrame(index=symbols,
                                    data={"type": security_types,
                                          "title": titles, "price": prices,
                                          "assets": assets, "cap": cap,
                                          "category": categories,
                                          "family": families})

    def get_security_data(self):
        """
        Collects all data and metadata for generated lists of ticker symbols.

        Wrapper method to assemble all closing prices and related metadata for
        every security extracted from the lists of ticker symbols and stored
        in a dataframe.  Duplicate symbols are removed from the dataframe
        based on security type with precedence in order of etf, fund, and
        stock.

        Returns
        -------
        data : obj
            All closing prices and related metadata for each ticker symbol.

        """
        p = multiprocessing.current_process().name
        self.__log.log("get security data %s" % p)
        results_list = list()
        # Loop over the configured dictionary of security lists
        for k in sorted(self.__options["eod_URL_dict"].keys()):
            type(self)._scraper.pattern_list = [
                self.__options["history_pattern"]]
            type(self)._scraper.findall = True
            # Create a concurrent process pool to execute the scraping of
            # symbol lists starting with each letter of the alphabet in
            # parallel
            with securitiesanalysis.utilities.NonDaemonicPool(
                    processes=self.__options["eod_pool_size"]) as pool:
                results_list.extend(pool.starmap(
                    self.scrape_eod,
                    [(k % letter, self.__options["eod_URL_dict"][k])
                     for letter in string.ascii_uppercase]))
        data = pandas.concat(results_list)
        # Sorted alphabetically to ensure that when duplicate symbols are found
        # that exchange traded funds take precedence, then mutual funds
        data.sort_values("type", inplace=True)
        # Remove any duplicate symbols keeping the first occurrence of each
        data = data[~data.index.duplicated(keep="first")]
        data.sort_index(inplace=True)
        data.index.name = "symbol"
        self.__log.log("got security data %s" % p)
        return data

    def __update_history(self, symbol, price):
        """
        Updates security history file with latest daily closing price.

        Appends the latest daily closing price to the history file defined by
        the associated ticker symbol.

        Parameters
        ----------
        symbol : str
            Ticker symbol representing security.
        price : float
            Daily closing price of security.

        """
        p = multiprocessing.current_process().name
        self.__log.log("updating history for %s on %s with %s %s"
                       % (symbol, str(self.__log_date), price, p))
        with codecs.open(os.path.join(self.__history_path, "%s.txt" % symbol),
                         "a+", "utf-8") as history_file:
            # Appends the current closing price to the security history file
            history_file.write("%s %s\n" % (str(self.__log_date), price))
        self.__log.log("updated history for %s on %s with %s %s"
                       % (symbol, str(self.__log_date), price, p))

    def __get_splits(self):
        """
        Retrieves information about recent splits.

        Downloads and extracts available split information filtering resultant
        list to past and current dates.

        Returns
        -------
        splits : obj
            Collection of split dates and number of before and after shares.

        """
        p = multiprocessing.current_process().name
        self.__log.log("get splits %s" % p)
        data = type(self)._scraper.scrape(self.options["split_URL"])
        symbol = [d[0] for d in data[0]]
        # Convert split dates into float values for later comparisons
        split_date = [round(
            securitiesanalysis.utilities.get_yearfrac(
                datetime.datetime.strptime(d[1], "%m/%d/%Y").date()),
            6) for d in data[0]]
        # Number of shares before the split occurred
        before = [float(d[2]) for d in data[0]]
        # Number of resulting shares after the split occurred
        after = [float(d[3]) for d in data[0]]
        splits = pandas.DataFrame(index=symbol,
                                  data={"date": split_date,
                                        "before": before,
                                        "after": after})
        # Only keep the recorded splits occurring before the following day for
        # processing to avoid applying splits multiple times
        splits = splits[
            splits["date"] <= securitiesanalysis.utilities.get_yearfrac(
                self.__log_date + datetime.timedelta(days=1))]
        self.__log.log("got splits %s" % p)
        return splits

    def __split_update(self, symbol, before, after, split_date):
        """
        Corrects security history file daily closing prices given split.

        Adjusts the daily closing prices for the passed in ticker symbol before
        the split occurred to maintain accurate regression fits.

        Parameters
        ----------
        symbol : str
            Ticker symbol representing security.
        before : str
            Number of shares involved before date of split.
        after : str
            Number of shares involved after date of split.
        split_date : str
            Date split occurs.

        """
        p = multiprocessing.current_process().name
        self.__log.log(
            "split update %s %s %s %s %s" % (symbol, before, after, split_date,
                                             p))
        # Check to determine if closing price history has been collected for
        # passed in symbol
        if os.path.exists(os.path.join(self.__history_path,
                                       "%s.txt" % symbol)):
            if "%s %s %s %s" % (
                    symbol, before, after,
                    split_date) in self.__applied_split_set:
                self.__log.log(
                    "already applied split %s %s %s %s %s" % (
                        symbol, before, after, split_date, p))
            else:
                history = pandas.read_csv(
                    os.path.join(self.__history_path, "%s.txt" % symbol),
                    sep=" ", header=None, names=["price"],
                    index_col=0)
                old_index = history.index
                # Convert history dates into float values for later comparisons
                history.index = [securitiesanalysis.utilities.get_yearfrac(
                    datetime.datetime.strptime(h, "%Y-%m-%d").date()
                ) for h in history.index]
                # Multiply the closing prices occurring before the split date
                # by the ratio of the before and after number of shares
                history["price"] = [round(
                    row["price"] * after / before, 2
                ) if index < split_date else row["price"]
                                    for index, row in history.iterrows()]
                history.index = old_index
                history.to_csv(os.path.join(
                    self.__history_path, "%s.txt" % symbol),
                    sep=" ", header=None, encoding="utf-8")
                self.__log.log(
                    "updating applied splits with %s %s %s %s %s" % (
                        symbol, before, after, split_date, p)
                )
                self.message_list.append(
                    "updating applied splits with %s %s %s %s" % (
                        symbol, before, after, split_date)
                )
                self.__applied_split_set.add(
                    "%s %s %s %s" % (symbol, before, after, split_date)
                )
        else:
            self.__log.log(
                "no history to update prices for splits with %s" % symbol)
        self.__log.log("split updated %s %s %s %s %s" % (
            symbol, before, after, split_date, p))

    def execute(self):
        """
        Retrieves daily closing price and metadata for securities.

        Creates any needed directories before configuring the regular
        expression based web scraper, then downloads all daily closing prices
        and related metadata for the extracted lists of ticker symbols, and
        updates the history files accounting for any new split information to
        make appropriate adjustments.

        """
        self.__initialize_directories__()
        self.__configure_scraper__()
        self.__log.log("starting update for %s" % str(self.__log_date))
        self.__data = self.get_security_data()
        [self.__update_history(index, row["price"]) for index,
                                                        row in
         self.__data.iterrows()]
        # Update the regular expression patterns and attributes to collect
        type(self)._scraper.pattern_list = [self.options["split_pattern"]]
        type(self)._scraper.findall = True
        type(self)._scraper.groups = None
        [self.__split_update(
            index, row["before"], row["after"], row["date"]) for index,
                                                                 row in
            self.__get_splits().iterrows()]
        # Update the list of applied splits to avoid duplicate processing
        self.__options["applied_split_set"] = sorted(
            list(self.__applied_split_set))

    @property
    def data(self):
        """obj: All closing prices and metadata for each symbol."""
        return self.__data

    @property
    def message_list(self):
        """list: Any messages to be included in the body of the email."""
        return self.__message_list

    @property
    def options(self):
        """dictionary: Dictionary of configured options."""
        return self.__options

    @property
    def root_path(self):
        """str: Folder containing all relevant subdirectories."""
        return self.__root_path

    @property
    def log_date(self):
        """date: Day of execution."""
        return self.__log_date

    @property
    def log(self):
        """obj: Logging utility for standard operating messages."""
        return self.__log

    @property
    def err(self):
        """obj: Logging utility for unexpected messages."""
        return self.__err

    @property
    def scraper(self):
        """obj: Static regular expression based web scraping utility."""
        return type(self)._scraper


if __name__ == '__main__':
    pass
