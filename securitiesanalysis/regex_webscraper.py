#!/usr/bin/env python3
"""
Provides customizable regular expression based HTML scraping.

This module contains a utility class for consuming HTML documents according to
a list of regular expressions along with options to refine the matched results
and handles all potential exceptions thrown during processing by returning
default values where necessary.

The RegexWebScraper class consolidates all HTML document fetching, regular
expression parsing, and necessary retry logic for connection and timeout errors
thrown when attempting to load web page contents into memory.

Notes
-----
Securities Analysis is distributed under the GNU Affero General Public License
v3 (https://www.gnu.org/licenses/agpl.html) as open source software with
attribution required.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.  See the GNU Affero General Public License v3
(https://www.gnu.org/licenses/agpl.html) for more details.

Copyright (C) 2020 John Sonsini.  All rights reserved.  Source code available
under the AGPLv3.

"""
import re
import time

import requests.exceptions


class RegexWebScraper(object):
    """
    Parses web pages via regular expression matching.

    Wraps downloading of HTML documents to memory and regular expression based
    scraping to simplify handling of connection and timeout errors in addition
    to providing options to customize returned matches.

    """

    def __init__(self, pattern_list, timeout, delay_time, max_retries,
                 verify=False, findall=False, groups=None):
        """
        Prepares all needed instance variables for scraping.

        Sets up regular expressions and related options, retry logic
        parameters, and server certificate check.

        Parameters
        ----------
        pattern_list : list
            Collection of regular expressions to match against.
        timeout : int
            Number of seconds to allow GET request to return.
        delay_time : int
            Number of seconds to wait before retry.
        max_retries : int
            Number of attempts to allow GET to return response.
        verify : boolean
            Flag to check server TLS certificate on GET command.
        findall : boolean
            Flag to return all matches found in HTML document.
        groups : tuple
            Indices of subgroups from matches to filter returns.

        """
        self.__pattern_list = [re.compile(p) for p in pattern_list]
        """list: Collection of regular expressions to match against."""
        self.__timeout = timeout
        """int: Number of seconds to allow GET request to return."""
        self.__delay_time = delay_time
        """int: Number of seconds to wait before retry."""
        self.__max_retries = max_retries
        """int: Number of attempts to allow GET to return response."""
        self.__verify = verify
        """boolean: Flag to check server TLS certificate on GET command."""
        self.__findall = findall
        """boolean: Flag to return all matches found in HTML document."""
        self.__groups = groups
        """tuple: Indices of subgroups from matches to filter returns."""

    def scrape(self, url):
        """
        Wraps private scraping method by initializing retry count to zero.

        Public wrapper of recursive web scraping method that sets the starting
        number of attempts to download the HTML document into memory at zero.

        Parameters
        ----------
        url : str
            Address of web page to be scraped via regular expression matching.

        Returns
        -------
        list
            All matched strings based on configured regular expressions.

        """
        return self.__scrape_retry(url, 0)

    def __scrape_retry(self, url, retry_count):
        """
        Loads and scrapes HTML document according to configured options.

        Recursively attempts to load a web page up to a maximum number of
        retries where in each iteration a timeout duration is respected along
        with checking the server certificate if either is specified, then
        proceeds to match the contents against a list of regular expressions,
        returning all matches or select groups for each.

        Parameters
        ----------
        url : str
            Address of web page to be scraped via regular expression matching.
        retry_count : int
            Number of attempts to load web page into memory (does not include
            any ReadTimeout or ConnectionError exceptions thrown during
            processing).

        Returns
        -------
        matches : list
            All matched strings based on configured regular expressions.

        """
        try:
            # Retrieve the contents of the HTML document
            contents = requests.get(url, verify=self.__verify,
                                    timeout=self.__timeout).text
            try:
                if self.__findall:
                    # Extract all of the matches in the document
                    matches = [re.findall(p.pattern, contents)
                               for p in self.__pattern_list]
                else:
                    if self.__groups:
                        # Extract the configured groups for the first match
                        matches = [p.search(contents).group(*self.__groups)
                                   for p in self.__pattern_list]
                    else:
                        # Extract the first match for each pattern
                        matches = [p.search(contents).group()
                                   for p in self.__pattern_list]
            except:
                # Substitute an empty value for each pattern on error
                matches = [None] * len(self.__pattern_list)
        except requests.exceptions.ReadTimeout:
            time.sleep(self.__delay_time)
            return self.__scrape_retry(url, retry_count)
        except requests.exceptions.ConnectionError:
            time.sleep(self.__delay_time)
            return self.__scrape_retry(url, retry_count)
        except:
            retry_count += 1
            if retry_count < self.__max_retries:
                time.sleep(self.__delay_time)
                return self.__scrape_retry(url, retry_count)
            else:
                return [None] * len(self.__pattern_list)
        else:
            return matches

    @property
    def verify(self):
        """boolean: Flag to check server TLS certificate on GET command."""
        return self.__verify

    @verify.setter
    def verify(self, verify):
        self.__verify = verify if isinstance(verify, bool) else self.__verify

    @property
    def pattern_list(self):
        """list: Collection of regular expressions to match against."""
        return self.__pattern_list

    @pattern_list.setter
    def pattern_list(self, pattern_list):
        try:
            temp = [re.compile(p) for p in pattern_list]
        except:
            temp = self.__pattern_list
        self.__pattern_list = temp

    @property
    def timeout(self):
        """int: Number of seconds to allow GET request to return."""
        return self.__timeout

    @timeout.setter
    def timeout(self, timeout):
        self.__timeout = timeout if 0 < timeout else self.__timeout

    @property
    def delay_time(self):
        """int: Number of seconds to wait before retry."""
        return self.__delay_time

    @delay_time.setter
    def delay_time(self, delay_time):
        self.__delay_time = delay_time if 0 < delay_time else self.__delay_time

    @property
    def max_retries(self):
        """int: Number of attempts to allow GET to return response."""
        return self.__max_retries

    @max_retries.setter
    def max_retries(self, max_retries):
        self.__max_retries = \
            max_retries if 0 < max_retries else self.__max_retries

    @property
    def groups(self):
        """tuple: Indices of subgroups from matches to filter returns."""
        return self.__groups

    @groups.setter
    def groups(self, groups):
        if groups is None:
            self.__groups = groups
        elif isinstance(groups, tuple):
            if all([isinstance(g, int) and -1 < g for g in groups]):
                self.__groups = groups

    @property
    def findall(self):
        """boolean: Flag to return all matches found in HTML document."""
        return self.__findall

    @findall.setter
    def findall(self, findall):
        self.__findall = findall if isinstance(
            findall, bool) else self.__findall


if __name__ == '__main__':
    pass
