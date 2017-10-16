#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. currentmodule:: lostifier.db.shp
.. moduleauthor:: Tom Weitzel

Datasource implementation for shapefiles.
"""

from osgeo import ogr
from lostifier.db.datasource import GisDataSource
from lostifier.exception import OperationNotSupportedException


class ShpDataSource(GisDataSource):
    """
    Datasource implementation for shapefiles.
    """

    def __init__(self, path):
        """
        Constructor.

        :param path: The path to the shapefile.
        :type path: ``str``
        """
        super().__init__()
        self._shpfile = path
        self._datasource = None

    def open(self):
        """
        Opens up a connection to the given datasource, in this case cracks open the shapefile.

        :return: None
        """
        driver = ogr.GetDriverByName('ESRI Shapefile')
        self._datasource = driver.Open(self._shpfile)

    def get_layers(self):
        """
        Get all of the layers in the datasource.

        :return: A list of ogr layer objects.
        """
        layers = []
        layers.append(self._datasource.GetLayer())
        return layers

    def get_layer_by_name(self, name):
        """
        Get a layer with the given name.

        :param name: The name of the layer.
        :type name: ``str``
        :return: The layer.
        :rtype: An ogr layer object.
        """
        return self._datasource.GetLayerByName(name)

    def copy_layers(self, source, selector):
        """
        Copy layers from the given data source.

        :param source: Another instance of a GisDataSource derived class.
        :type source: :py:class``GisDataSource``
        :param selector: A function that can be used to select which layers to copy.
        :type selector: ``function``
        :return: A list of the names of the layers copied.
        :rtype: ``list[str]``
        """
        raise OperationNotSupportedException('copy_layers is not supported for shapefile data sources.')

