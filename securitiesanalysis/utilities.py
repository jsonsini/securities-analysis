#!/usr/bin/env python3
"""
Contains utility functions for generic use elsewhere in package.

Group of utility functions including error formatting, ordinal date generation,
mapping of traditional market capitalization categories, fit function for
nonlinear regression analysis, and adding a worksheet to an existing
workbook along with a basic logging class.

The nondemonic classes are simple wrappers to create the ability to instantiate
nested concurrent pools of processes.  The Logger class is a simple means of
prepending a formatted timestamp to messages before appending them to a
configured path.

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
import calendar
import datetime
import multiprocessing.pool
import re


def format_error(error):
    """
    Converts error into one line string representation.

    Extracts the exception type and message from the error to build a simple
    string summarizing the information to be used when logging.

    Parameters
    ----------
    error : tuple
        Exception type, exception message, and traceback information.

    Returns
    -------
    str
        Formatted version of error with type and message.

    """
    return "%s %s" % (error[0].__name__, error[1])


def get_yearfrac(d):
    """
    Converts date into floating point value for numerical comparison.

    Treats the year as an integer and calculates the day of the year divided by
    the total number of days in the respective year to add as a fractional
    component.

    Parameters
    ----------
    d : date
        Date to be converted into floating point value.

    Returns
    -------
    float
        Formatted version of error with type and message.

    """
    return d.year + (d.timetuple().tm_yday + 0.0) \
           / (366 if calendar.isleap(d.year) else 365)


def get_cap(assets):
    """
    Converts assets into corresponding market capitalization category.

    Returns the market capitalization category based on the provided net assets
    or market capitalization value.

    Parameters
    ----------
    assets : int
        Net Assets of mutual funds or Exchange Traded Funds, market
        capitalization of stocks.

    Returns
    -------
    str
        Formatted version of error with type and message.

    """
    if assets < 0:
        return "UNKNOWN"
    elif assets < 2000000000:
        return "small"
    elif assets < 10000000000:
        return "mid"
    else:
        return "large"


def func(x, a, b):
    """
    Model function for non linear regression fit.

    Function used by scipy.optimize.curve_fit method to calculate a and b
    coefficients minimizing squared error given independent and dependent
    variables.

    Parameters
    ----------
    x : float
        Independent variable of non linear regression fit.
    a : float
        First coefficient of non linear regression fit.
    b : float
        Second coefficient of non linear regression fit.

    Returns
    -------
    float
        Value of a * b ^ x.

    See Also
    --------
    scipy.optimize.curve_fit : Use non-linear least squares to fit a function
        to data.

    """
    return a * pow(b, x)


def add_sheet(workbook, name, frame, split_pattern):
    """
    Places a new spreadsheet in workbook and populates with dataframe values.

    Adds a sheet to an existing Excel compatible workbook inserting values
    from a dataframe into rows and columns as needed.

    Parameters
    ----------
    workbook : obj
        Representation of an Excel compatible workbook.
    name : str
        Title of spreadsheet.
    frame : obj
        Data to be iterated over and populate the spreadsheet.
    split_pattern : str
        Regular expression to separate values in each data frame row.

    """
    worksheet = workbook.add_worksheet(name)
    row = 0
    # Iterate over the dataframe rows
    for line in [re.split(split_pattern, f) for f in
                 frame.to_csv().split("\n")[:-1]]:
        column = 0
        # Iterate over the columns of each row
        for value in line:
            # Populate the cell with the corresponding value from the dataframe
            worksheet.write(row, column, value)
            column += 1
        row += 1


class NonDaemonicProcess(multiprocessing.Process):
    """
    Process class limited to non daemonic state.

    Extension of multiprocessing.Process with the daemon property modified to
    return false in all cases.

    """

    @property
    def daemon(self):
        """boolean: Explicitly restrict processes to be nondaemonic."""
        return False

    @daemon.setter
    def daemon(self, value):
        pass


class NonDaemonicContext(type(multiprocessing.get_context())):
    """
    Minimal context wrapping nondaemonic processes.

    Multiprocessing context that uses the NonDaemonicProcess class to create a
    pool containing only nondaemonic processes.

    """

    Process = NonDaemonicProcess
    """obj: Processes for pool limited to nondaemonic state."""


class NonDaemonicPool(multiprocessing.pool.Pool):
    """
    Minimal pool wrapping nondaemonic processes.

    Extension of multiprocessing.pool.Pool that uses the NonDaemonContext
    class to create a nestable pool containing only nondaemonic processes.

    """

    def __init__(self, *args, **kwargs):
        """
        Prepares all needed instance variables for execution.

        Adds the nondemonic context to allow for nesting of pools.

        """
        kwargs["context"] = NonDaemonicContext()
        super(NonDaemonicPool, self).__init__(*args, **kwargs)


class Logger(object):
    """
    Simple logging utility with customizable time stamp format.

    Provides minimal logging functionality, prepending chosen time stamp format
    to each logged message written to the file path specified.

    """

    def __init__(self, log_path, time_format="%Y-%m-%d %H:%M:%S.%f"):
        """
        Prepares all needed instance variables for execution.

        Sets up the logging path and time stamp format.

        Parameters
        ----------
        log_path : str
            Absolute path to log file.
        time_format : str
            Information from current time to be prepended to messages.

        """
        self.__log_path = log_path
        """str: Absolute path to log file."""
        self.__time_format = time_format
        """str: Information from current time to be prepended to messages."""

    def log(self, message):
        """
        Appends message to log file .

        Based on the previously specified absolute log path, concatenates the
        current time to the message and adds the resulting string to the file.

        Parameters
        ----------
        message : str
            String to be appended to log file.

        """
        with open(self.__log_path, "a+") as out:
            out.write("[%s] %s\n" % (
                datetime.datetime.now().strftime(self.__time_format),
                message))

    @property
    def log_path(self):
        """str: Absolute path to log file."""
        return self.__log_path

    @log_path.setter
    def log_path(self, log_path):
        self.__log_path = log_path

    @property
    def time_format(self):
        """str: Information from current time to be prepended to messages."""
        return self.__time_format

    @time_format.setter
    def time_format(self, time_format):
        self.__time_format = time_format


if __name__ == '__main__':
    pass
