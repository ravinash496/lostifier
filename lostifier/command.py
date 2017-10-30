#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. currentmodule:: lostifier.command
.. moduleauthor:: Tom Weitzel

Command pattern base-classes for loading things.
"""

from abc import ABCMeta, abstractmethod


class LoadCommand(object):
    """
    Base class for load operation commands.

    """
    __metaclass__ = ABCMeta

    def __init__(self, receiver: object, label: str):
        """
        Constructor.

        :param receiver: The object that will be called by the command.
        :param receiver: ``object``
        :param label: A label for the type of loader.
        :type label: ``str``
        """
        super().__init__()
        self._receiver = receiver
        self._label = label

    @property
    def label(self) -> str:
        """
        Gets the label for the loader command.

        :return: The label.
        :rtype: ``str``
        """
        return self._label

    @abstractmethod
    def execute(self):
        """"
        Abstract execute method to be implemented by sub-classes.

        """
        pass


class LoadInvoker(object):
    """
    Object that invokes the load command.
    """
    def __init__(self):
        """
        Constructor.

        """

    def execute(self, command: LoadCommand):
        """
        Executes the given load command.

        :param command: The load command to run.
        :type command: :py:class:`LoadCommand`
        """
        command.execute()

