import time
import datetime
import json
import boto3
import functools
import audit_log



# def exception_handler(function):
#     """
#     Decorator that handles exceptions caught in functions
#     :param function:
#     :return:
#     """

    # @functools.wraps(function)
    # def wrapper(job, *args, **kwargs):
    #     ...
    #     return
    #
    # return

def exception_handler(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        with open("logs.txt", "a") as f:
            f.write("Executed: " + func + " at " + str(datetime.datetime.now()) + "\n")
            val = func(*args, **kwargs)
        return val

    return wrapper

# def logging_method(title):
#     """
#     Decorator
#     :param title:
#     :return:
#     """

    # def decorator(function):
    #     @functools.wraps(function)
    #     def wrapper(job, *args, **kwargs):
    #         ...
    #         return
    #
    #     return
    #
    # return


def log(title):
    @functools.wraps(title)
    def wrapper(*args, **kwargs):
        with open("logs.txt", "a") as f:
            f.write("Executed: " + title + " at " + str(datetime.datetime.now()) + "\n")
            val = title(*args, **kwargs)
        return val

    return wrapper





class ETLFramework(object):
    """
    Class that does something
    """

    def __init__(self):
        self.curr_time = None

    @logging_method('Initialize step')
    def initialize(self):
        """
        intializes the class
        :return:
        """
        self.curr_time = time.now()

    # TODO add audit log method

    @logging_method('Extract step')
    def extract(self):
        """
        intializes the class
        :return:
        """
        print('Extract method not implemented.')

    @logging_method('Transform step')
    def transform(self):
        """
        intializes the class
        :return:
        """
        print('Transform method not implemented.')

    @logging_method('Load step')
    def load(self):
        """
        intializes the class
        :return:
        """
        print('Load method not implemented.')

    def audit_record_end(self):
        return

    @exception_handler
    def run(self):
        self.initialize()
        self.audit_record_start()
        self.extract()
        self.transform()
        self.load()
        self.audit_record_end()