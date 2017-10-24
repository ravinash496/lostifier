#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. currentmodule:: exception
.. moduleauthor:: Tom Weitzel

All of the custom exceptions for lostifier operations.
"""


class LostifierException(Exception):
    """
    Base class for all lostifier exceptions.
    """
    def __init__(self, message, nested=None):
        """
        Constructor

        :param message: A text message associated with the exception.
        :type message: ``str``
        :param nested: An optional nested exception.
        :type nested: :py:class:`Exception`
        """
        super(LostifierException, self).__init__(message)
        self._message = message
        self._nested = nested


class OperationNotSupportedException(LostifierException):
    """
    Exception class for when an operation is not supported.
    """
    def __init__(self, message, nested=None):
        """
        Constructor

        :param message: A text message associated with the exception.
        :type message: ``str``
        :param nested: An optional nested exception.
        :type nested: :py:class:`Exception`
        """
        super(OperationNotSupportedException, self).__init__(message, nested)


class InvalidParameterException(LostifierException):
    """
    Exception class for cases where not all required parameters were passed or were invalid.
    """
    def __init__(self, message, nested=None):
        """
        Constructor

        :param message: A text message associated with the exception.
        :type message: ``str``
        :param nested: An optional nested exception.
        :type nested: :py:class:`Exception`
        """
        super(InvalidParameterException, self).__init__(message, nested)