#!/usr/bin/env python3
"""
Performs regression analysis on securities and saves the results.

This module processes all of the currently updated security history files to
filter on range of available data for each and calculates multiple regression
fits where possible to store the results in the appropriate directories and
send a summary message via email to the configured recipient upon completion.

The SecuritiesAnalysis class is the main class for determining the
coefficients of non linear regression fits for various trailing durations of
daily closing prices and the trends of those coefficients.

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
import codecs
import copy
import datetime
import email.mime.application
import email.mime.multipart
import email.mime.text
import itertools
import math
import multiprocessing
import pydoc
import os
import smtplib
import socket
import sys
import time

import numpy
import pandas
import scipy.optimize
import scipy.stats.mstats
import sklearn.metrics
import xlsxwriter

import securitiesanalysis.utilities


class SecuritiesAnalysis(object):
    """
    Generates regression fits and trends for all securities.

    Calculates the coefficients of the non linear regression fits for each
    security, gathers previous output, performs linear regression to determine
    trends of the previous coefficients, saves the results and summary, and
    message via email to the configured recipient.

    """

    def __init__(self, root_path, options, data, message_list,
                 log_date, log, err):
        """
        Prepares all needed instance variables for execution.

        Sets up all paths, logging, the message list, options, and previously
        collected data.

        Parameters
        ----------
        root_path : str
            Folder containing all relevant subdirectories.
        options : dictionary
            Dictionary of configured options.
        data : obj
            All closing prices and metadata for each symbol.
        message_list : list
            Any messages to be included in the body of the email.
        log_date : date
            Day of execution.
        log : obj
            Logging utility for standard operating messages.
        err : obj
            Logging utility for unexpected messages.

        """
        self.__root_path = root_path
        """str: Folder containing all relevant subdirectories."""
        self.__log_path = os.path.join(root_path, "logs")
        """str: Folder containing log message files."""
        self.__error_path = os.path.join(root_path, "errors")
        """str: Folder containing error message files."""
        self.__history_path = os.path.join(root_path, "history")
        """str: Folder containing security history files."""
        self.__report_path = os.path.join(root_path, "reports")
        """str: Folder containing all output directories and files."""
        self.__options = options
        """dictionary: Dictionary of configured options."""
        self.__data = data
        """obj: All closing prices and metadata for each symbol."""
        self.__message_list = message_list
        """obj: List of messages to be included in body of summary email."""
        self.__log_date = log_date
        """date: Day of execution."""
        self.__log = log
        """obj: Logging utility for standard operating messages."""
        self.__err = err
        """obj: Logging utility for unexpected messages."""
        # Converts strings to corresponding classes for results dataframe
        self.__collect_types = {
            k: pydoc.locate(v) for k,
                                   v in self.__options["collect_types"].items()}
        """dictionary: Types for each column of dataframe."""
        self.__define_ranges__()
        pandas.set_option("display.float_format", lambda x: "%.6f" % x)

    def __define_ranges__(self):
        """
        Sets up date ranges for linear and non linear regression fits.

        Determines the trailing durations for the last three years along with
        the second and third most recent years and the most recent one, three,
        and six month and one year periods for the trends of the non linear
        coefficients.

        """
        ranges = list(
            itertools.combinations(
                [d.to_pydatetime().date() for d in
                 pandas.date_range(datetime.date(self.__log_date.year - 3,
                                                 self.__log_date.month,
                                                 self.__log_date.day),
                                   self.__log_date,
                                   freq=pandas.DateOffset(years=1)
                                   ).tolist()],
                2))
        # Only keep the date ranges as stipulated above
        self.__ranges = pandas.DataFrame(
            index=["3Y", "3YD", "2Y", "2YD", "1Y"],
            data={
                "start": [
                    securitiesanalysis.utilities.get_yearfrac(r[0])
                    for r in ranges[:1] + ranges[2:]],
                "end": [
                    securitiesanalysis.utilities.get_yearfrac(r[1])
                    for r in ranges[:1] + ranges[2:]]})
        summary_ranges = pandas.date_range(
            end=self.__log_date, periods=13,
            freq=pandas.DateOffset(months=1)).tolist()
        # Only keep the one, three, and six month and one year date ranges
        summary_ranges = [
            (securitiesanalysis.utilities.get_yearfrac(summary_ranges[i]),
             securitiesanalysis.utilities.get_yearfrac(self.__log_date))
            for i in [0, 6, 9, 11]]
        self.__summary_ranges = pandas.DataFrame(
            index=["1Y", "6M", "3M", "1M"],
            data={"start": [r[0] for r in summary_ranges],
                  "end": [r[1] for r in summary_ranges]})

    def get_fit(self, history, symbol, duration):
        """
        Calculates non linear regression fit for passed in data.

        Determines the best fit for the model y = a * (b ^ x) where the x
        variable is the date normalized by first value and the y variable is
        the daily closing price.

        Parameters
        ----------
        history : obj
            Daily closing prices with corresponding dates.
        symbol : str
            Ticker symbol representing security.
        duration : str
            Period for data, based on index of self.__ranges dataframe.

        Returns
        -------
        fit : float
            Coefficient representing mantissa of exponential component (b).
        r2 : float
            Coefficient of determination for predicted values given fit.
        rmse : float
            Root mean square error of predicted values given fit.

        """
        try:
            p = multiprocessing.current_process().name
            self.__log.log("get fit for %s %s %s" % (symbol, duration, p))
            # Provide default values in case fit is not able to be calculated
            fit = numpy.nan
            r2 = numpy.nan
            rmse = numpy.nan
            # Normalize the history so the independent variable starts at zero
            history.index = [i - history.index[0] for i in history.index]
            popt, _ = scipy.optimize.curve_fit(
                securitiesanalysis.utilities.func, history.index,
                history["price"])
            # Capture the b term (rate of growth) from the y = a * (b ^ x) fit
            fit = popt[1]
            # Generate the dependent variable values based on the fit function
            predict = [securitiesanalysis.utilities.func(h, *popt)
                       for h in history.index]
            r2 = sklearn.metrics.r2_score(history["price"], predict)
            rmse = math.sqrt(
                sklearn.metrics.mean_squared_error(history["price"], predict))
            self.__log.log("found fit for %s %s %s %s %s %s"
                           % (symbol, duration, fit, r2, rmse, p))
        except:
            self.__err.log(
                "get fit generic error for %s %s %s %s"
                % (symbol, duration,
                   securitiesanalysis.utilities.format_error(sys.exc_info()),
                   p)
            )
        return fit, r2, rmse

    def process_history(self, symbol):
        """
        Generates return ratios and regression fits for each security.

        Retrieves history, finds the closest dates to the range boundaries,
        and calculates both the actual price ratios and the non linear
        regression fits over the configured durations.

        Parameters
        ----------
        symbol : str
            Ticker symbol representing security.

        Returns
        -------
        actual : list
            Ratio of starting and ending prices for history durations.
        fit : list
            Non linear regression fit coefficients, coefficient of
                determination values, and root mean squared errors for history
                durations.

        """
        try:
            p = multiprocessing.current_process().name
            self.__log.log("process history for %s %s" % (symbol, p))
            # Provide default values in case periods do not have complete data
            actual = [numpy.nan for i in range(5)]
            fit = [3 * [numpy.nan] for i in range(5)]
            history = pandas.read_csv(
                os.path.join(self.__history_path, "%s.txt" % symbol),
                sep=" ", header=None, names=["price"], index_col=0)
            history.index = [securitiesanalysis.utilities.get_yearfrac(
                datetime.datetime.strptime(h, "%Y-%m-%d").date()
            ) for h in history.index]
            # Determine if data exists to fill each date range completely
            fill_period = [
                history.index[0] <= self.__ranges["start"][i] for i in
                self.__ranges.index]
            # Find the dataframe indices closest to the range boundaries
            closest_indices = [(history.index.get_loc(
                self.__ranges["start"][i], method="nearest"),
                                history.index.get_loc(self.__ranges["end"][i],
                                                      method="nearest"))
                               if fill_period[i] else numpy.nan for i in
                               range(len(fill_period))]
            closest_dates = [(history.index[c[0]],
                              history.index[c[1]]) if c is not numpy.nan
                             else numpy.nan for c in closest_indices]
            # Calculate the ratio of start and end price for each period
            actual = [
                "%.6f"
                % (1 + (history.loc[d[1]]["price"]
                        / history.loc[d[0]]["price"] - 1) / (d[1] - d[0]))
                if d is not numpy.nan and not
                history.loc[d[0]]["price"] == 0 else numpy.nan for d in
                closest_dates]
            # Generate the fits of the same periods and collect the growth rate
            fit = [["%.6f"
                    % v for v in self.get_fit(
                history[self.__ranges["start"]
                        [i]:self.__ranges["end"][i]],
                symbol, self.__ranges.index[i])] if fill_period[i]
                   else 3 * [numpy.nan] for i in range(len(fill_period))]
            self.__log.log("processed history for %s %s %s %s"
                           % (symbol, str(actual), str(fit), p))
        except:
            self.__err.log("process history generic error for %s %s %s"
                           % (symbol,
                              securitiesanalysis.utilities.format_error(
                                  sys.exc_info()), p))
        return actual, fit

    def get_regression_coefficients(self):
        """
        Collects price ratios and regression fits in dataframe.

        Iterates over all securities, gathering the actual ratios, non
        linear regression fits, coefficient of determination values, and root
        mean squared errors for each to add appropriate fields to the
        dataframe.

        """
        p = multiprocessing.current_process().name
        self.__log.log("get regression coefficients %s" % p)
        actual_fit = [self.process_history(i) for i in self.__data.index]
        # Extract the nested tuple elements into separate lists of lists
        # The elements of each are lists of values over the defined periods
        actual = [af[0] for af in actual_fit]
        fit = [[i[0] for i in af[1]] for af in actual_fit]
        r2 = [[i[1] for i in af[1]] for af in actual_fit]
        rmse = [[i[2] for i in af[1]] for af in actual_fit]
        # Extract the list for each period and value for the dataframe
        self.__data["3YA"], self.__data["3YDA"], self.__data["2YA"], \
        self.__data["2YDA"], self.__data["1YA"] = \
            tuple([[a[i] for a in actual] for i in range(5)])
        self.__data["3YF"], self.__data["3YDF"], self.__data["2YF"], \
        self.__data["2YDF"], self.__data["1YF"] = \
            tuple([[f[i] for f in fit] for i in range(5)])
        self.__data["3YR2"], self.__data["3YDR2"], self.__data["2YR2"], \
        self.__data["2YDR2"], self.__data["1YR2"] = \
            tuple([[r[i] for r in r2] for i in range(5)])
        self.__data["3YRMSE"], self.__data["3YDRMSE"], self.__data["2YRMSE"], \
        self.__data["2YDRMSE"], self.__data["1YRMSE"] = \
            tuple([[r[i] for r in rmse] for i in range(5)])
        self.__log.log("got regression coefficients %s" % p)

    def collect_reports(self):
        """
        Gathers prior stored results over past year.

        Loads previous reports into dataframes based on the configured field
        order and limited to the most recent year.

        Returns
        -------
        reports : list
            Dataframes containing all collected data along with date collected
            for past year.

        """
        p = multiprocessing.current_process().name
        self.__log.log(
            "collecting reports for %s %s" % (str(self.__log_date), p))
        # Generate a list of previous report paths
        report_paths = [
            (os.path.join(self.__report_path, "data", r),
             securitiesanalysis.utilities.get_yearfrac(
                 datetime.datetime.strptime(r[:-4],
                                            "%Y-%m-%d").date()
             )) for r in os.listdir(os.path.join(self.__report_path, "data"))]
        # Filter the list to just over the prior year's worth of reports
        reports = [(pandas.read_csv(r[0], sep="|", header=0,
                                    names=self.__options["column_order"],
                                    dtype=self.__collect_types, index_col=0),
                    r[1]
                    ) for r in sorted(report_paths) if
                   securitiesanalysis.utilities.get_yearfrac(
                       self.__log_date - datetime.timedelta(days=375)) <= r[1]]
        self.__log.log(
            "collected reports for %s %s" % (str(self.__log_date), p))
        return reports

    def convert_reports(self, reports):
        """
        Assembles previous reports into single dataframe.

        Adds date of collection as a column to each dataframe before
        consolidating them into a single dataframe.

        Parameters
        ----------
        reports : list
            Dataframes containing all collected data along with date collected
            for past year.

        Returns
        -------
        concatenated_reports : obj
            All data and metadata collected over past year.

        """
        p = multiprocessing.current_process().name
        self.__log.log(
            "converting reports for %s %s" % (str(self.__log_date), p))
        coverted_reports = []
        # Adds a column with the collected date to each dataframe in the list
        while reports:
            f, d = reports.pop(0)
            f["date"] = f.shape[0] * [d]
            coverted_reports.append(f)
        if coverted_reports:
            # Generate one dataframe for all the collected reports
            concatenated_reports = pandas.concat(coverted_reports)
        else:
            # An empty dataframe is needed in the event there is no data
            columns = copy.deepcopy(self.__options["column_order"])
            columns.append("date")
            concatenated_reports = pandas.DataFrame(columns=columns)
        self.__log.log(
            "converted reports for %s %s" % (str(self.__log_date), p))
        return concatenated_reports

    def group_reports(self, reports):
        """
        Separates all report data into list for each symbol.

        Creates list of dataframes, one for each symbol, of all collected data
        and includes the date of collection for each row.

        Parameters
        ----------
        reports : obj
            All data and metadata collected over past year.

        Returns
        -------
        grouped_reports : list
            Dataframes grouped by symbol of all collected data and metadata.

        """
        p = multiprocessing.current_process().name
        self.__log.log(
            "grouping reports for %s %s" % (str(self.__log_date), p))
        # Create list of dataframes based on ticker symbol
        grouped_reports = [group for _,
                                     group in reports.groupby(reports.index)]
        # Move the date column to the index for each dataframe
        for i in range(len(grouped_reports)):
            symbol = grouped_reports[i].index[0]
            temp = grouped_reports[i]
            temp.index = temp.pop("date")
            grouped_reports[i] = (symbol, temp)
        self.__log.log(
            "grouped reports for %s %s" % (str(self.__log_date), p))
        return grouped_reports

    def get_summary_fit(self, summary, symbol, duration):
        """
        Calculates linear regression fit for passed in non linear coefficients.

        Determines the best fit for the model y = a + b * x where the x
        variable is the date normalized by first value and the y variable is
        the non linear regression coefficients.

        Parameters
        ----------
        summary : obj
            Non linear regression coefficients and corresponding dates.
        symbol : str
            Ticker symbol representing security.
        duration : str
            Period for data, based on index of self.__summary_ranges dataframe.

        Returns
        -------
        slope : float
            Linear regression coefficient representing rate of change multiple
            in the x variable.

        """
        try:
            p = multiprocessing.current_process().name
            self.__log.log(
                "get summary fit for %s %s %s" % (symbol, duration, p))
            # Provide a default value in case the slope cannot be calculated
            slope = numpy.nan
            # Normalize the index so the independent variable starts at zero
            summary.index = [i - summary.index[0] for i in summary.index]
            # Generate the linear slope of the regression fit
            slope, _, _, _, _ = scipy.stats.mstats.linregress(summary.index,
                                                              summary.values)
            self.__log.log("got summary fit for %s %s %s %s" % (symbol,
                                                                duration,
                                                                slope, p))
        except:
            self.__err.log(
                "get summary fit generic error for %s %s %s %s" % (
                    symbol, duration,
                    securitiesanalysis.utilities.format_error(sys.exc_info()),
                    p))
        return slope

    def process_summary(self, report):
        """
        Generates ratios and regression fits for each coefficient.

        Finds the closest dates to the summary range boundaries and
        calculates both the actual coefficient ratios and the linear
        regression fits over the configured summary durations.

        Parameters
        ----------
        report : tuple
            Ticker symbol and dataframe containing non linear regression
            coefficients

        Returns
        -------
        symbol : str
            Ticker symbol representing security.
        actual : list
            Ratio of starting and ending coefficients for summary durations.
        fit : list
            Linear regression fit coefficients for summary durations.

        """
        p = multiprocessing.current_process().name
        try:
            symbol, frame = report
            self.__log.log("process summary for %s %s" % (symbol, p))
            c = self.__options["process_summary_columns"]
            # Provide default values in case periods do not have complete data
            actual, fit = tuple(
                2 * [dict(zip(c,
                              [4 * [numpy.nan] for i in range(len(c))]))]
            )
            # Determine if data exists to fill each date range completely
            fill_period = [
                frame.index[0] <= self.__summary_ranges["start"][i] for i in
                self.__summary_ranges.index]
            # Find the dataframe indices closest to the range boundaries
            closest_indices = [
                (frame.index.get_loc(self.__summary_ranges["start"][i],
                                     method="nearest"),
                 frame.index.get_loc(self.__summary_ranges["end"][i],
                                     method="nearest")) if fill_period[i]
                else numpy.nan for i in range(len(fill_period))]
            closest_dates = [
                (frame.index[ci[0]], frame.index[ci[1]])
                if ci is not numpy.nan else numpy.nan
                for ci in closest_indices]
            # Calculate the ratio of start and end fit for each period
            actual = {
                s: ["%.6f"
                    % (1 + (frame.loc[d[1]][s] / frame.loc[d[0]][s] - 1)
                       / (d[1] - d[0])) if d is not numpy.nan and
                                           not frame.loc[d[0]][
                                                   s] == 0 else numpy.nan
                    for d in closest_dates] for s in c}
            # Generate the fits of the same periods and collect the linear rate
            fit = {
                s: ["%.6f"
                    % (self.get_summary_fit(frame[s][closest_dates[i][0]:
                                                     closest_dates[i][1]],
                                            symbol, s))
                    if fill_period[i] else numpy.nan
                    for i in range(len(fill_period))] for s in c}
            self.__log.log("processed summary for %s %s" % (symbol, p))
        except:
            self.__err.log("process summary generic error for %s %s %s" %
                           (symbol,
                            securitiesanalysis.utilities.format_error(
                                sys.exc_info()), p))
        return symbol, actual, fit

    def get_summary_regression_coefficients(self, reports):
        """
        Collects coefficient ratios and regression fits into dataframe.

        Iterates over all securities, gathering the actual coefficient ratios
        and linear regression fits for each to create a dataframe for the
        results.

        Parameters
        ----------
        reports : list
            Ticker symbols and dataframes containing non linear regression
            coefficients

        Returns
        -------
        results : obj
            Linear regression coefficients for all securities and summary
            durations.

        """
        p = multiprocessing.current_process().name
        self.__log.log("get summary regression coefficients %s" % p)
        # Extract the nested tuple elements into separate lists of dictionaries
        # The elements are dictionaries for each of the defined periods
        summary_results = [self.process_summary(r) for r in reports]
        symbol = [s[0] for s in summary_results]
        actual = [s[1] for s in summary_results]
        fit = [s[2] for s in summary_results]
        results_dict = dict()
        s = list(self.__summary_ranges.index)
        # Extract the list for each period and value for the dataframe
        for c in self.__options["process_summary_columns"]:
            for i in range(4):
                results_dict["%s-%sA" % (c, s[i])] = [actual[j][c][i] for j in
                                                      range(len(actual))]
                results_dict["%s-%sF" % (c, s[i])] = [fit[j][c][i] for j in
                                                      range(len(fit))]
        results_dict["symbol"] = symbol
        results = pandas.DataFrame.from_dict(results_dict, dtype=numpy.float16)
        # Move the symbol column to the index
        results.index = results.pop("symbol")
        self.__log.log("got summary regression coefficients %s" % p)
        return results

    def aggregate(self):
        """
        Groups records in dataframe and generates summary statistics for each.

        Selects regression fits for securities with the highest one year
        returns by type and calculates the aggregate regression fit summary
        statistics across the entire market and for each column to group
        results by.

        Returns
        -------
        top_sorted_by_type_data : obj
            Regression fits for top one year returns by security type and cap.
        market : obj
            Aggregate regression fits for entire market.
        group_dict : dictionary
            Collection of aggregate regression fits for category, family, type,
            and cap groupings.

        """
        p = multiprocessing.current_process().name
        self.__log.log("starting aggregation of results %s" % p)
        # Find the top ten one year returns for each security type and cap
        top_sorted = self.__data.sort_values(
            ["type", "cap", "1YA"], ascending=[True, False, False]).groupby(
            ["type", "cap"], as_index=False).head(10)
        top_sorted.index.name = "symbol"
        a = self.__options["aggregate_dict"]
        # Generate counts, means, and standard deviations for the whole market
        market = self.__data.aggregate(a)
        market = market[self.__options["column_order"][7:]]
        market.index.name = "market"
        groups = ["category", "family", "type", "cap"]
        group_dict = dict()
        # Generate counts, means, and standard deviations for the listed groups
        for g in groups:
            group_dict[g] = self.__data.groupby(g).aggregate(a)
            # Flatten the multi-level aggregate column names
            group_dict[g].columns = [
                " ".join(c).strip() for c in group_dict[g].columns.values]
            group_dict[g] = group_dict[g][self.__options["aggregate_columns"]]
            group_dict[g].sort_values("1YA mean", ascending=False,
                                      inplace=True)
        self.__log.log("finished aggregation of results %s" % p)
        return top_sorted, market, group_dict

    def process_fits(self):
        """
        Groups records in dataframe and generates summary statistics for each.

        Selects regression fits for securities with the fastest increasing non
        linear regression coefficients by type and calculates the
        corresponding aggregate regression fit summary statistics for each
        category.

        Returns
        -------
        fit_dict : dictionary
            Collection of top sorted regression fits by type and aggregate
            summary statistics of regression fits by category.

        """
        p = multiprocessing.current_process().name
        self.__log.log("processing fits %s" % p)
        fit_dict = dict()
        a = self.__options["aggregate_dict"]
        for f in self.__options["fit_columns"]:
            # Collects the fastest 100 increasing fits for each security type
            fit_dict[f] = self.__data.sort_values(
                f, ascending=False).groupby("type",
                                            as_index=False).head(100)
            fit_dict[f].index.name = f
            fc = "%s category" % f
            # Generate counts, means, and standard deviations for each category
            fit_dict[fc] = self.__data.groupby("category").aggregate(a)
            # Flatten the multi-level aggregate column names
            fit_dict[fc].columns = [
                " ".join(c).strip() for c in fit_dict[fc].columns.values]
            fit_dict[fc] = fit_dict[fc][self.__options["aggregate_columns"]]
            fit_dict[fc].sort_values("%s mean" % f, ascending=False,
                                     inplace=True)
        self.__log.log("processed fits %s" % p)
        return fit_dict

    def generate_workbook(self, top_sorted, market, group_dict, fit_dict):
        """
        Stores summary of regression fits into spreadsheets.

        Gathers all of the sorted regression fits and aggregate statistical
        groupings by metadata columns to save as an Excel compatible workbook.

        Parameters
        ----------
        top_sorted : obj
            Regression fits for top one year returns by security type and cap.
        market : obj
            Aggregate regression fits for entire market.
        group_dict : dictionary
            Collection of aggregate regression fits for category, family, type,
            cap groupings.
        fit_dict : dictionary
            Collection of top sorted regression fits by type and aggregate
            summary statistics of regression fits by category.

        """
        p = multiprocessing.current_process().name
        self.__log.log("generating workbook %s" % p)
        # Generate a list of dataframes to populate the workbook
        results = [
            top_sorted, market, group_dict["category"], group_dict["family"],
            group_dict["type"], group_dict["cap"]]
        results.extend([fit_dict[f] for f in self.__options["fit_columns"]])
        results.extend([fit_dict["%s category" % f]
                        for f in self.__options["fit_columns"]])
        workbook = xlsxwriter.Workbook(
            os.path.join(self.__report_path, "summary",
                         "%s.xlsx" % str(self.__log_date)))
        # Iterate over the dataframe list to populate each workbook page
        [securitiesanalysis.utilities.add_sheet(
            workbook, h, r, self.__options["unquoted_comma_pattern"])
         for h, r in zip(self.__options["result_headers"], results)]
        workbook.close()
        self.__log.log("generated workbook %s" % p)

    def get_email_message(self):
        """
        Composes email message with brief notes and attaches summary workbook.

        Generates message containing the list of symbols with new split
        information since previous execution and attaches summary workbook.

        Returns
        -------
        summary_message : obj
            Representation of email message with from and to addresses,
            subject, body, and attachments included.

        """
        p = multiprocessing.current_process().name
        self.__log.log("getting email message for %s %s"
                       % (str(self.__log_date), p))
        email_address = self.__options["email_address"]
        summary_message = email.mime.multipart.MIMEMultipart()
        summary_message["Subject"] = "market summary for %s" \
                                     % str(self.__log_date)
        summary_message["From"] = email_address
        summary_message["To"] = email_address
        # Attach the summary workbook to the message
        with codecs.open(os.path.join(
                self.__report_path, "summary",
                "%s.xlsx" % str(self.__log_date)), "rb") as summary_file:
            attachment = email.mime.application.MIMEApplication(
                summary_file.read(), Name="%s.xlsx" % str(self.__log_date))
        attachment["Content-Disposition"] = \
            "attachment; filename=\"%s.xlsx\"" % str(self.__log_date)
        summary_message.attach(
            email.mime.text.MIMEText("\n".join(self.__message_list)))
        summary_message.attach(attachment)
        self.__log.log("got email message for %s %s"
                       % (str(self.__log_date), p))
        return summary_message

    def email_results(self, summary_message):
        """
        Sends summary email to configured address.

        Delivers email containing list of recent splits, and unmapped
        categories encountered during processing, and summary workbook to
        the recipient specified in the configuration file.

        Parameters
        -------
        summary_message : obj
            Representation of email message with from and to addresses,
            subject, body, and attachments included.

        """
        p = multiprocessing.current_process().name
        self.__log.log("sending email for %s %s" % (str(self.__log_date), p))
        email_address = self.__options["email_address"]
        sent = False
        retry_count = 0
        while not sent:
            try:
                s = smtplib.SMTP(self.__options["smtp_host"],
                                 self.__options["smtp_port"])
                # Initialize the SMTP sever
                s.ehlo()
                s.starttls()
                s.ehlo()
                s.login(email_address, self.__options["email_password"])
                s.sendmail(email_address, [email_address],
                           summary_message.as_string())
                s.quit()
                sent = True
                self.__log.log("sent email %s %s" % (str(self.__log_date), p))
            except socket.error:
                self.__err.log(
                    "socket error for sending email %s" % retry_count)
                time.sleep(self.__options["delay_time"])
            except smtplib.SMTPServerDisconnected:
                self.__err.log(
                    "SMTP server disconnected error sending email %s"
                    % retry_count)
                time.sleep(self.__options["delay_time"])
            except:
                self.__err.log(
                    "generic error sending email %s" % retry_count)
                retry_count += 1
                if retry_count < self.__options["max_retry_count"]:
                    time.sleep(self.__options["delay_time"])
                else:
                    sent = True

    def remove_logs(self):
        """
        Deletes log and error files based on date.

        Removes log and error files from their corresponding directories given
        the threshold specified in the configuration file.

        """
        p = multiprocessing.current_process().name
        self.__log.log("removing logs %s" % p)
        keep_date = self.__log_date \
                    - datetime.timedelta(days=self.__options["log_keep_days"])
        log_files = [os.path.join(self.__log_path, l)
                     for l in os.listdir(self.__log_path)]
        # Extract the file dates from the names
        log_dates = {
            datetime.datetime.strptime(
                l.split("/")[-1][:-4],
                "%Y-%m-%d").date(): l for l in log_files}
        # Find the dates outside of the retention period
        keep = [k for k in log_dates.keys() if keep_date < k]
        # Delete the log files outside of the retention period
        [os.remove(v) for k, v in log_dates.items() if k not in keep]
        removed_files = [v for k, v in log_dates.items() if k not in keep]
        error_files = [os.path.join(self.__error_path, l)
                       for l in os.listdir(self.__error_path)]
        error_dates = {
            datetime.datetime.strptime(
                l.split("/")[-1][:-4],
                "%Y-%m-%d").date(): l for l in error_files}
        # Delete the error files outside of the retention period
        [os.remove(v) for k, v in error_dates.items() if k not in keep]
        removed_files.extend(
            [v for k, v in error_dates.items() if k not in keep])
        self.__log.log("deleted %s based on %s day threshold"
                       % (str(removed_files), self.__options["log_keep_days"]))
        self.__log.log("removed logs %s" % p)

    def execute(self):
        """
        Generates regression fits and summarizes results in workbook and email.

        Calculates regression fits for all securities across various durations,
        generates aggregate summary statistics by configured groupings, saves
        results and summary workbook locally, and sends summary email to
        configured address.

        """
        self.get_regression_coefficients()
        collected_reports = self.collect_reports()
        converted_reports = self.convert_reports(collected_reports)
        grouped_reports = self.group_reports(converted_reports)
        # Join the nonlinear and linear fits into one dataframe
        d = self.__data.merge(
            self.get_summary_regression_coefficients(grouped_reports),
            how="left", left_index=True, right_index=True, sort=True)
        self.__data = d[self.__options["column_order"]]
        self.__data.to_csv(os.path.join(self.__report_path, "data",
                                        "%s.txt" % str(self.__log_date)),
                           sep="|", encoding="utf-8")
        # Read the previously saved dataframe to ensure correct column types
        self.__data = pandas.read_csv(
            os.path.join(self.__report_path, "data",
                         "%s.txt" % str(self.__log_date)), sep="|",
            header=0, names=self.__options["column_order"], index_col=0)
        top_sorted, market, group_dict = self.aggregate()
        fit_dict = self.process_fits()
        self.generate_workbook(top_sorted, market, group_dict, fit_dict)
        summary_message = self.get_email_message()
        self.email_results(summary_message)
        self.remove_logs()
        self.__log.log("finished update for %s" % str(self.__log_date))

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


if __name__ == '__main__':
    pass
