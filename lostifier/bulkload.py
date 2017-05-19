#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. currentmodule:: bulkload
.. moduleauthor:: Vishnu Reddy, Darell Stoick

Reads data from a .GDB and imports it into a Postgres DB for the ECRF/LVF. 
Two modes 
F -Full - Overwrites and current values and imports the full .gdb 
C - Change only - Reads an Add and Delete table for each layer, and processes each entry accordingly(Add/Update/Delete)  
"""
import psycopg2 as psycopg2
from osgeo import ogr
from osgeo import gdal

def connectpostgresdb(serverName, database, port, usr, pw):
    """
    Create a connection to postgres DB
    
    :param serverName: Host name (IP or Server Name)
    :param database: DB name
    :param port: DB Port
    :param usr: Username
    :param pw: Password
    :return: psycopg2 connection object
    """

    try:
        connection_string = "host=" + serverName + " user=" + usr + " password=" + pw + " dbname=" + database + " port=" + port
        con = psycopg2.connect(connection_string)
        # print("Postgres connection successfull")

    except psycopg2.Error as e:
        print("Error : " + str(e.pgerror))

    return con

# End of connectpostgresdb()

def changeOnlyGDBImport():
    """
    Starting Location for the Change Only Process
        
    :return: 
    """

    # Open a connection to the PostGIS database.
    servername = input("Please enter the server name(host):")
    # servername = 'localhost'
    database = input("Please enter the database name:")
    # database = 'bulkloadtest'
    port = input("Please enter the port for the database:")
    # port = '5432'
    usr = input("Please enter the db username:")
    # usr = 'postgres'
    pw = input("Please enter the db password:")
    # pw = '*'

    connectionstring = "PG:dbname='%s' host='%s' port='%s' user='%s' password='%s'" % (database, servername, port, usr, pw)
    print(connectionstring)
    ogrds = ogr.Open(connectionstring, 1)

    if ogrds is None:
        print("Could not connect to database.")
        quit()

    # Start Transaction
    ogrds.StartTransaction()

    # For each layer name in our list of layers to load . . .
    for name in __layers_to_load:

        ProcessLayer(name, ogrds)

    for i in range(__ds_gdb.GetLayerCount()):
        layer = __ds_gdb.GetLayerByIndex(i)
        layername = layer.GetName()
        layername = str(layername)
        if layername.upper().startswith("ESB") or layername.upper().startswith("ALOC"):
            # We have found a layer which matches ESB<type> or ALOC<type>
            # Trim off _add or _delete from the layer name and pass it to ProcessLayer()
            trimedlayername = layername.strip('_add')
            trimedlayername = layername.strip('_del')

            ProcessLayer(trimedlayername, ogrds)

    # Commit transaction
    ogrds.CommitTransaction()
    print("All changes have been processed.")

# End of changeOnlyGDBImport()


def ProcessLayer(name, ogrds):
    """
    Process the adds and deletes for the layer
    :param name: 
    :param ogrds: 
    :return: 
    """
    # Get the layer_del from the file geodatabase.
    gdblayer_del = __ds_gdb.GetLayerByName(name + '_del')

    # If the layer was found, loop though the items in the layer
    if gdblayer_del is not None:

        deleteitemfromgdb(gdblayer_del, name, ogrds)

    # Get the layer_add from the file geodatabase.
    gdblayer_add = __ds_gdb.GetLayerByName(name + '_add')

    # If the layer was found, loop though the items in the layer
    if gdblayer_add is not None:

        additemfromgdb(gdblayer_add, name, ogrds)

# End of ProcessLayer()

def deleteitemfromgdb(gdbLayer_del, name, ogrds):
    """
    Deletes an existing item from the postgres DB
    :param gdbLayer_del: 
    :param name: 
    :param ogrds: 
    :return: 
    """

    itemcount = 0
    feature = gdbLayer_del.GetNextFeature()
    while feature is not None:
        featureid = feature.GetFieldAsString(feature.GetFieldIndex("gcUnqID"))
        ogrds.ExecuteSQL("Delete FROM %s where gcunqid = '%s'" % (name, featureid), None, "")

        itemcount = itemcount + 1
        feature = gdbLayer_del.GetNextFeature()

    print("'%s' items were deleted from '%s'" % (itemcount, name))

# End of deleteitmefromgdb()

def additemfromgdb(gdblayer_add, name, ogrds):
    """
    Inserts a new item or Updates an existing item in the postgres DB
    :param gdblayer_add: 
    :param name: 
    :param ogrds: 
    :return: 
    """

    itemcount = 0
    feature = gdblayer_add.GetNextFeature()
    while feature is not None:
        postgreslayer = ogrds.GetLayerByName(name)
        # Clear FID so postgres will autogenerate next available in the sequence
        feature.SetFID(-1)
        gcunqid = feature.GetFieldAsString('gcunqid')

        featureitems = ogrds.ExecuteSQL("SELECT * FROM %s where gcunqid = '%s' " % (name, gcunqid), None, "")
        fcount = featureitems.GetFeatureCount(1)

        if fcount == 0:
            # Create the new item
            result = postgreslayer.CreateFeature(feature)
        else:
            # Remove item which has matching gcunqid
            ogrds.ExecuteSQL("Delete FROM %s where gcunqid = '%s'" % (name, gcunqid), None, "")
            # Create the new item
            result = postgreslayer.CreateFeature(feature)

        verifyResults(result, gcunqid)
        itemcount = itemcount + 1
        feature = gdblayer_add.GetNextFeature()

    print("'%s' items were added into '%s'" % (itemcount, name))

# End of additemfromgdb()

def verifyResults(result, gcunqid):
    """
    Verify an error code was not thrown by CreateFeature()
    :param result: 
    :param gcunqid: 
    :return: 
    """
    if result != 0:
        raise NameError('Process failed while trying to add item:' + gcunqid)

# End of VerifyResults()

def fullGDBImport():
    """
    Process imports the full GDB overwriting any previous values
    :return: 
    """

    # Open a connection to the PostGIS database.
    servername = input("Please enter the server name(host):")
    # serverName = 'localhost'
    database = input("Please enter the database name:")
    # database = 'gis'
    port = input("Please enter the port for the database:")
    # port = '5432'
    usr = input("Please enter the db username:")
    # usr = 'postgres'
    pw = input("Please enter the db password:")
    # pw = '*'
    connectionstring = "PG:dbname='%s' host='%s' port='%s' user='%s' password='%s'" % (database, servername, port, usr, pw)
    print(connectionstring)

    ogrds = ogr.Open(connectionstring,1)


    if ogrds is None:
        print("Could not connect to database.")
        quit()

    options = []
    options.append('OVERWRITE=YES')

    # For each layer name in our list of layers to load . . .
    for name in __layers_to_load:
        # Get the layer from the file geodatabase.
        layer = __ds_gdb.GetLayerByName(name)

        # If the layer was found, copy it to the DB.
        if layer is not None:
            layername = layer.GetName()
            print("Importing layer :: " + layername + "!")
            tablename = ogrds.CopyLayer(layer, name, options).GetName()
            __layersList_Processed.append(tablename)

    print("--Looking for layers starting with ESB and ALOC--")

    for i in range(__ds_gdb.GetLayerCount()):
        layer = __ds_gdb.GetLayerByIndex(i)
        layername = layer.GetName()
        layername = str(layername)
        if layername.upper().startswith("ESB") or layername.upper().startswith("ALOC"):
            print("Importing layer :: " + layername + "!")
            tablename = ogrds.CopyLayer(layer, layername, options).GetName()
            __layersList_Processed.append(tablename)


# ogrds.ExecuteSQL("") would not run the setval command so we created a new connection
# with psycopg2 to run the SQL statement
    try:
        con = connectpostgresdb(servername, database, port, usr, pw)
        con.autocommit = True
        cursor = con.cursor()

        for processedlayer in __layersList_Processed:
            sqlstring = "SELECT setval(pg_get_serial_sequence('%s', 'ogc_fid'), max(ogc_fid)) FROM %s;" % (processedlayer, processedlayer)
            cursor.execute(sqlstring)
            print("Postgres primary key sequence has been reset for the table %s" % (processedlayer))

    except psycopg2.Error as e:
        print(e.pgerror)
    finally:
        con.close()


# End of fullGDBImport()


# ***********************************
# Start of the Script
# ***********************************

# The list of layers we want to load.
__layers_to_load = ['CountyBoundary', 'UnIncCommBoundary', 'IncMunicipalBoundary', 'StateBoundary', 'RoadCenterline', 'SSAP']
__layersList_Processed =[]

# Open up the file geodatabase.
try:
    driver = ogr.GetDriverByName('OpenFileGDB')
    response = input("Please enter the complete source path for gdb file:")
    __ds_gdb = driver.Open(response, 0)
except Exception as e:
    print(e)

if __ds_gdb is None:
    print("Could not load gdb file, please verify path is correct.")
    quit()

scriptmode = input("Select bulk import mode. Type 'C' for change only or type 'F' for a full gbd import.")

try:
    if scriptmode.lower() == 'c':
        changeOnlyGDBImport()
    elif scriptmode.lower() == 'f':
        fullGDBImport()
    else:
        print("Invalid option detected, goodbye")
except NameError:
    print('An error was encountered and the process has been terminated.')
    raise