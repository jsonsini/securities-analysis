# Securities Analysis
Securities Analysis is a Python package designed to provide customizable least squares regression analysis for dynamic lists of mutual funds, exchange traded funds, and stocks based on their daily closing prices.

### Installation
To install Securities Analysis, simply execute the following command from the download directory:
```bash
sudo python setup.py install
```

To execute Securities Analysis from the command line run the following:
```bash
securities-analysis
```

### Infrastructure
The directory structure under a customizable root folder is as follows:
 * History - Text files with two columns of data (date and closing price) for each ticker symbol available each day
 * Reports - All generated output
 -- Data - All data collected each day indexed by ticker symbol, one pipe delimited text file per day
 -- Summary - Selection of highest performing securities and aggregations by various metadata, these spreadsheets are emailed to the configured address once daily processing has completed
 * Logs - Messages indicating normal operation, useful for debugging unexpected results
 * Errors - Messages reflecting issues during execution, most often due to network connection or read timeout errors

The root folder is specified in the options.json located in the installation directory under the data subdirectory.

### Collected Data/Metadata
The list of securities is generated at run time by scraping a collection of webpages configured via the options.json file, the default set of pages are from [http://eoddata.com](http://eoddata.com) and the regular expression to extract relevant information is specified by the `history_pattern` entry.  Both can be replaced with a custom mapping of websites indexed by letter and the appropriate regular expression to extract ticker symbols, titles, and daily closing prices.

```json
"eod_URL_dict" : {
"http://eoddata.com/stocklist/AMEX/%s.htm": "etf",
"http://eoddata.com/stocklist/USMF/%s.htm": "fund",
"http://eoddata.com/stocklist/NASDAQ/%s.htm": "stock",
"http://eoddata.com/stocklist/NYSE/%s.htm": "stock"
}
```

```json
"history_pattern": "(?<=Chart for ).*,([^\"]+)\">.*<\\/A><\\/td><td>(.*)<\\/td>.*<\\/td><td align=right>.*<\\/td><td align=right>(.*)<\\/td><td align=right>"
```

The following metadata is collected for all securities:
 * Assets - Net Assets for Mutual Funds and Exchange Traded Funds, Market Capitalization for Stocks
 * Cap - Large, Mid, or Small defined by greater than $10B, between $2B and $10B, and less than $2B
 * Category - Sector or grouping ticker symbol belongs to, standard mapping is defined in the options file such that any unmapped categories encountered will be added to a message list and included in the subsequent email for the summary
 * Family - Only for Mutual Funds and Exchange Traded Funds, the investment firm managing the fund

Should any metadata not be available for any particular security the default value for Assets is -1 and "UNKNOWN" for the rest.

### Regression Analysis
Varying durations for the analysis are the trailing 1, 2, and 3 year periods along with the second and third most recent years (the last two are non overlapping, e.g. if 1/1/2018 to 1/1/2019 was the most recent year then 1/1/2017 to 1/1/2018 and 1/1/2016 to 1/1/2017 would be the second and third most recent years)

All dates are converted into floating point values based on the following formula:

![equation](https://latex.codecogs.com/gif.latex?Year%20&plus;%20%5Cfrac%7BDay%5C%3A%20of%5C%3A%20Year%7D%7B%5Cbegin%7Bcases%7D%20366%20%26%20%5Ctext%7B%20if%20%7D%20Year%20%3D%20Leap%5C%3A%20Year%5C%5C%20365%20%26%20%5Ctext%7B%20if%20%7D%20Year%20%5Cneq%20Leap%5C%3A%20Year%20%5Cend%7Bcases%7D%7D)

Dates are treated as the independent variable and normalized such that the beginning of each period is 0, the corresponding daily closing prices are the dependent variable.

For each duration's worth of data the best fit for the following coefficients is determined via the [SciPy](https://scipy.org/) [`optimize.curve_fit method`](https://docs.scipy.org/doc/scipy/reference/generated/scipy.optimize.curve_fit.html):

![equation](https://latex.codecogs.com/gif.latex?y%20%3D%20a%20*%20b%20%5E%20x)

where
 * x = dates converted into floating point values as defined above
 * y = corresponding daily closing prices for a specific security

The ![equation](https://latex.codecogs.com/gif.latex?b) coefficient represents the best fit growth rate of the security such that if

![equation](https://latex.codecogs.com/gif.latex?b%20%3D%201.1)

then the most likely daily closing price after one year will be 110% of the original closing price.

After determining the ![equation](https://latex.codecogs.com/gif.latex?b) coefficient for all current securities, linear least squares regression analysis is performed on the coefficients themselves for varying durations, effectively calculating the trend of the best fits and included in the output.  The linear least squares regression fit is generated via the [SciPy](https://scipy.org/) [`stats.mstats.linregress`](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.mstats.linregress.html) method:

![equation](https://latex.codecogs.com/gif.latex?y%20%3D%20a%20&plus;%20b%20*%20x)

where
 * x = dates converted into floating point values as defined above
 * y = exponential coefficients from non linear least squares regression analysis above

### Known Limitations
Stocks and Exchange Traded Funds are updated to reflect any splits, at this time a free and consolidated source for Mutual Fund splits was not found so price increases/decreases due to those splits are not corrected in the history files for the corresponding ticker symbols.

### Additional Information
The accumulated history of all securities from the beginning of 2015 onwards is available in tar.gz and zip formats for downloading in the event users would like to benefit from pre-collected data.  No code relies on these files, they are simply to be decompressed in the history subdirectory of the configured root folder before the first execution.  If another source using the same format is used instead care should be taken to update the applied splits portion of the options.json file to reflect the status of associated stocks and exchange traded funds correctly.

### License Information
Securities Analysis is distributed under the [GNU Affero General Public License v3](https://www.gnu.org/licenses/agpl.html) as open source software with attribution required.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the [GNU Affero General Public License v3](https://www.gnu.org/licenses/agpl.html) for more details.

Copyright (C) 2022 John Sonsini.  All rights reserved.  Source code available under the AGPLv3.
