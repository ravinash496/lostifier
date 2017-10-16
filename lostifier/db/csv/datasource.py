#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. currentmodule:: lostifier.db.csv.datasource
.. moduleauthor:: Tom Weitzel

Datasource for CSV files.ds
"""

import csv
from lostifier.db.datasource import TabularDataSource
from lostifier.exception import OperationNotSupportedException


class CsvDataSource(TabularDataSource):
    """
    Data source for csv files.
    """

    def __init__(self, path, has_header=False):
        """
        Nothing to see here.
        """
        super().__init__()
        self._path = path
        self._has_header = has_header
        self._headers = []
        self._rows = []

    def open(self):
        """
        Opens up a csv file.

        :param path: The path to the csv file.
        :type path: ``str`
        :param has_header Indicates if a header row is expected or not.
        :type has_header ``bool``
        :return: None
        """
        with open(self._path) as csv_file:
            csv_reader = csv.reader(csv_file)
            if self._has_header:
                header = next(csv_reader)
                self._headers.extend(header)

            for row in csv_reader:
                self._rows.append(row)

    def copy_rows(self, source, selector=None):
        """
        Copy rows from the given data source.

        :param source: Another instance of a TabularDataSource derived class.
        :type source: :py:class`TabularDataSource`
        :param selector: A function that can be used to select which rows to copy.
        :type selector: ``function``
        :return:
        """
        raise OperationNotSupportedException('copy_rows not implemented for csv files yet.')

    def get_field_names(self):
        """
        Get the names of the fields if available.
        :return: A list of the names of the fields.
        :rtype: ``list[str]``
        """
        return self._headers

    def get_rows(self, start_index, count):
        """
        Get the content of rows.

        :param start_index: The zero-based index of the starting row.
        :type start_index: ``int``
        :param count: The number of rows to fetch.
        :type count: ``int``
        :return: A list containing the content of specified rows.
        :rtype: ``list[list[str]]``
        """
        end_index = start_index + count
        if end_index > len(self._rows):
            return self._rows[start_index:]
        else:
            return self._rows[start_index:end_index]

