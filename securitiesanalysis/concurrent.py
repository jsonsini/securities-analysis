#!/usr/bin/env python3
"""
Wraps functions in asynchronous processes and pools for concurrent execution.

This module contains all of the concurrency framework used throughout the rest
of the package and establishes total decoupling between the wrapped function
logic and all parallelization.

The NonDaemonicProcess and NonDaemonicPool classes are minimal extensions of
multiprocessing.Process and multiprocessing.pool.Pool respectively, the
FunctionWrapper class provides asynchronous execution of any passed function,
and the Pool class creates a bulk parallelization of any passed function.

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
import datetime
import multiprocessing.pool
import sys
import time


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


class NonDaemonicPool(multiprocessing.pool.Pool):
    """
    Minimal pool wrapping nondaemonic processes.

    Extension of multiprocessing.pool.Pool that uses the NonDaemonicProcess
    class to create a pool containing only nondaemonic processes.

    """

    Process = NonDaemonicProcess
    """obj: Processes for pool limited to nondaemonic state."""


class FunctionWrapper(object):
    """
    Wraps function in asynchronous process for execution.

    Provides asynchronous execution of a function, capturing a return or error
    value, and respecting any timeout limitation imposed along with a default
    return value used either for exceeding the timeout or due to the wrapped
    function throwing an error.

    Note
    ----
    This class is for wrapping functions and not methods, due to pickling
    limitations instance methods should not be passed to this wrapper, instead
    methods should be moved to the global level outside of any class.

    """

    def __init__(self, handle, process_name, timeout=None,
                 default_return=None, *args, **kwargs):
        """
        Prepares all needed instance variables for execution.

        Sets up nondaemonic pool, function return value, error message, and
        done status.

        Parameters
        ----------
        handle : function
            Function to be executed.
        process_name : str
            Name of process wrapped function executes within.
        timeout : float
            Number of seconds to allow wrapped function to execute.
        default_return : object
            Return value of wrapped function if timeout or error occurs.
        args : list
            List of unnamed arguments passed to the wrapped function.
        kwargs : dictionary
            Dictionary of named arguments passed to the wrapped function.

        """
        self.__handle = handle
        """function: Function to be executed."""
        self.__process_name = process_name
        """str: Name of process wrapped function executes within."""
        self.__timeout = timeout
        """float: Number of seconds to allow wrapped function to execute."""
        self.__default_return = default_return
        """obj: Return value of function if timeout or error occurs."""
        self.__args = args
        """list: List of unnamed arguments passed to the wrapped function."""
        self.__kwargs = kwargs
        """dictionary: Dictionary of named arguments passed to the function."""
        self.__pool = NonDaemonicPool(processes=1,
                                      initializer=self.name_process,
                                      initargs=[self.__process_name],
                                      maxtasksperchild=1)
        """obj: Asynchronous process to execute wrapped function."""
        self.__return_value = None
        """obj: Value returned from wrapped function."""
        self.__error = None
        """str: String representation of error raised by function."""
        self.__done = False
        """boolean: Flag to signify wrapped function has finished execution."""

    def execute(self):
        """
        Executes function asynchronously and handles result.

        Wraps execution of the function in an asynchronous process while
        respecting any time limit if given and captures either the return or
        error value depending on successful completion, throwing an error, or
        exceeding the timeout period in which case a default value is returned
        if provided.

        """
        try:
            if self.__timeout:
                limit = datetime.datetime.now() \
                    + datetime.timedelta(seconds=self.__timeout)
                # Submit the function to the pool for asynchronous execution
                async_result = self.__pool.apply_async(
                    self.__handle, self.__args, self.__kwargs)
                self.__pool.close()
                # Wait for the function to complete or the timeout to pass
                while not async_result.ready() and \
                        datetime.datetime.now() < limit:
                    time.sleep(0.001)
                if async_result.ready():
                    # On successful completion store the return value
                    self.__return_value = async_result.get()
                else:
                    self.__pool.terminate()
                    if self.__default_return:
                        # Assign the default if needed and provided
                        self.__return_value = self.__default_return
                self.__pool.join()
                self.__done = True
            else:
                # Submit the function to the pool for asynchronous execution
                async_result = self.__pool.apply_async(
                    self.__handle, self.__args, self.__kwargs)
                self.__pool.close()
                # Wait for the function to complete
                while not async_result.ready():
                    time.sleep(0.001)
                # On successful completion store the return value
                self.__return_value = async_result.get()
                self.__pool.join()
                self.__done = True
        except:
            self.__pool.terminate()
            # On error store the raised exception
            self.__error = str(sys.exc_info())
            self.__pool.join()
            self.__done = True

    @property
    def done(self):
        """boolean: Flag to signify wrapped function has finished execution."""
        return self.__done

    @done.setter
    def done(self, done):
        self.__done = done

    def name_process(self, process_name):
        """
        Assigns name to process executing function.

        Sets the current process name to the passed in value.

        Parameters
        ----------
        process_name : str
            Name of process wrapped function executes within.

        """
        multiprocessing.current_process().name = process_name

    @property
    def process_name(self):
        """str: Name of process wrapped function executes within."""
        return self.__process_name

    @process_name.setter
    def process_name(self, process_name):
        self.__process_name = process_name

    @property
    def return_value(self):
        """obj: Value returned from wrapped function."""
        return self.__return_value

    @return_value.setter
    def return_value(self, return_value):
        self.__return_value = return_value

    @property
    def error(self):
        """str: String representation of error raised by function."""
        return self.__error

    @error.setter
    def error(self, error):
        self.__error = error

    @property
    def timeout(self):
        """float: Number of seconds to allow wrapped function to execute."""
        return self.__timeout

    @timeout.setter
    def timeout(self, timeout):
        self.__timeout = timeout if 0 < timeout else self.__timeout

    @property
    def default_return(self):
        """obj: Return value of function if timeout or error occurs."""
        return self.__default_return

    @default_return.setter
    def default_return(self, default_return):
        self.__default_return = default_return


def _func(wrapper_tuple):
    """
    Executes function in asynchronous wrapper.

    Extracts needed parameters for a FunctionWrapper instance and executes the
    contained function, waiting until it is finished before returning the
    wrapped function return value and error message.

    Parameters
    ----------
    wrapper_tuple : tuple
        Function handle, parameters, and related values for asynchronous
        execution.

    Returns
    -------
    obj
        Value returned from wrapped function.
    str
        String representation of error raised by wrapped function.

    """
    handle = wrapper_tuple[0]
    name = wrapper_tuple[1]
    timeout = wrapper_tuple[2]
    default_value = wrapper_tuple[3]
    # Unnamed arguments (tuple) to be passed to wrapped function
    args = wrapper_tuple[4]
    # Named arguments (dictionary) to be passed to wrapped function
    kwargs = wrapper_tuple[5]
    w = FunctionWrapper(handle, name, timeout, default_value, *args, **kwargs)
    w.execute()
    # Wait until the done flag is set to true
    while not w.done:
        time.sleep(0.001)
    return w.return_value, w.error


class Pool(object):
    """
    Process pool for concurrent execution of functions.

    Provides concurrent execution limited by the chosen pool size for a passed
    list of functions and collects either the return value or a string
    representation of the error thrown during execution for each.

    """

    def __init__(self, wrapper_tuples, pool_size):
        """
        Prepares all needed instance variables for execution.

        Sets up wrapper tuples, pool size, return and error value lists, and
        the process pool for concurrent execution.

        Parameters
        ----------
        wrapper_tuples : list
            Function handles, parameters, and details for execution.
        pool_size : int
            Maximum number of processes to execute concurrently.

        """
        self.__wrapper_tuples = wrapper_tuples
        """list: Function handles, parameters, and details for execution."""
        self.__pool_size = pool_size
        """int: Maximum number of processes to execute concurrently."""
        self.__return_values = None
        """list: Values returned from wrapped functions."""
        self.__errors = None
        """list: String representations of errors raised by functions."""
        self.__pool = NonDaemonicPool(processes=self.__pool_size,
                                      maxtasksperchild=1)
        """obj: Process pool for concurrent execution of functions."""

    def execute(self):
        """
        Executes functions concurrently.

        Wraps concurrent execution of the list of functions in the wrapper
        tuples and for each one stores either the return or error value
        depending on successful completion or throwing an error.

        """
        # Submit the wrapped functions to the pool for asynchronous execution
        return_tuples = self.__pool.map(_func, self.__wrapper_tuples)
        self.__pool.close()
        self.__pool.join()
        # Collect the return and error values from each function
        self.__return_values, self.__errors = zip(*return_tuples)

    @property
    def pool_size(self):
        """int: Maximum number of processes to execute concurrently."""
        return self.__pool_size

    @pool_size.setter
    def pool_size(self, size):
        self.__pool_size = size if 0 < size else self.__pool_size

    @property
    def wrapper_tuples(self):
        """list: Function handles, parameters, and details for execution."""
        return self.__wrapper_tuples

    @wrapper_tuples.setter
    def wrapper_tuples(self, wrapper_tuples):
        self.__wrapper_tuples = wrapper_tuples

    @property
    def return_values(self):
        """list: Values returned from wrapped functions."""
        return self.__return_values

    @return_values.setter
    def return_values(self, return_values):
        self.__return_values = return_values

    @property
    def errors(self):
        """list: String representations of errors raised by functions."""
        return self.__errors

    @errors.setter
    def errors(self, errors):
        self.__errors = errors


if __name__ == '__main__':
    pass
