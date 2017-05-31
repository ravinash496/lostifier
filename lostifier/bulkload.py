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

    createPrimaryKey(servername,database,port,usr,pw)
    createSequence(servername,database,port,usr,pw)
    createIndex(servername,database,port,usr,pw)


# End of fullGDBImport()


def createPrimaryKey(servername, database, port, usr, pw):
    """
    Alters each table's primary key to gcunqid field
    :param servername: 
    :param database: 
    :param port: 
    :param usr: 
    :param pw: 
    :return: 
    """

    try:
        con = connectpostgresdb(servername, database, port, usr, pw)
        con.autocommit = True
        cursor = con.cursor()

        for processedlayer in __layersList_Processed:
            sqlstring = "ALTER TABLE %s DROP CONSTRAINT %s_pkey;" % (processedlayer,processedlayer)
            cursor.execute(sqlstring)

            sqlstring = "ALTER TABLE %s ADD PRIMARY KEY (gcunqid)" % (processedlayer)
            cursor.execute(sqlstring)

            print("Primary key has been set to gcunqid for the table %s" % (processedlayer))

    except psycopg2.Error as e:
        print(e.pgerror)
    finally:
        con.close()

# End of createPrimaryKey()

def createSequence(servername,database,port,usr,pw):
    """
    Update sequence values for the tables in postgres.
    :param servername: 
    :param database: 
    :param port: 
    :param usr: 
    :param pw: 
    :return: 
    """
    # ogrds.ExecuteSQL("") would not run the setval command so we created a new connection
    # with psycopg2 to run the SQL statement
    try:
        con = connectpostgresdb(servername, database, port, usr, pw)
        con.autocommit = True
        cursor = con.cursor()

        for processedlayer in __layersList_Processed:
            sqlstring = "SELECT setval(pg_get_serial_sequence('%s', 'ogc_fid'), max(ogc_fid)) FROM %s;" % (
            processedlayer, processedlayer)
            cursor.execute(sqlstring)
            print("Postgres primary key sequence has been reset for the table %s" % (processedlayer))

    except psycopg2.Error as e:
        print(e.pgerror)
    finally:
        con.close()

# End of createSequence

def createIndex(servername,database,port,usr,pw):
    """
    Apply Index to specific fields in postgres tables
    :param servername: 
    :param database: 
    :param port: 
    :param usr: 
    :param pw: 
    :return: 
    """
    try:
        con = connectpostgresdb(servername, database, port, usr, pw)
        con.autocommit = True
        cursor = con.cursor()

        sqlstring = getIndexString()
        cursor.execute(sqlstring)
        print("Index's have been applied.")

    except psycopg2.Error as e:
        print(e.pgerror)
    finally:
        con.close()

# End of createIndex

def getIndexString():
    """
    Creates a string containting the full SQL command for all of the index's
    :return: string
    """

    value = """-- Index: srgis.public.ssap_srcfulladr_idx
-- DROP INDEX srgis.public.ssap_srcfulladr_idx;
CREATE INDEX ssap_srcfulladr_idx
  ON srgis.public.ssap
  USING btree
  (btrim(upper(srcfulladr::text)));

-- Index: srgis.public.ssap_addnum_idx
-- DROP INDEX srgis.public.ssap_addnum_idx;
CREATE INDEX ssap_addnum_idx
  ON srgis.public.ssap
  USING btree
  (btrim(upper(addnum::text)));

-- Index: srgis.public.ssap_country_idx
-- DROP INDEX srgis.public.ssap_country_idx;
CREATE INDEX ssap_country_idx
  ON srgis.public.ssap
  USING btree
  (btrim(upper(country::text)));

-- Index: srgis.public.ssap_county_idx
-- DROP INDEX srgis.public.ssap_county_idx;
CREATE INDEX ssap_county_idx
  ON srgis.public.ssap
  USING btree
  (btrim(upper(county::text)));

-- Index: srgis.public.ssap_msagcomm_idx
-- DROP INDEX srgis.public.ssap_msagcomm_idx;
CREATE INDEX ssap_msagcomm_idx
  ON srgis.public.ssap
  USING btree
  (btrim(upper(msagcomm::text)));

-- Index: srgis.public.ssap_postcomm_idx
-- DROP INDEX srgis.public.ssap_postcomm_idx_idx;
CREATE INDEX ssap_postcomm_idx
  ON srgis.public.ssap
  USING btree
  (btrim(upper(postcomm::text)));

-- Index: srgis.public.ssap_postdir_idx
-- DROP INDEX srgis.public.ssap_postdir_idx;
CREATE INDEX ssap_postdir_idx
  ON srgis.public.ssap
  USING btree
  (btrim(upper(postdir::text)));

-- Index: srgis.public.ssap_predir_idx
-- DROP INDEX srgis.public.ssap_predir_idx;
CREATE INDEX ssap_predir_idx
  ON srgis.public.ssap
  USING btree
  (btrim(upper(predir::text)));

-- Index: srgis.public.ssap_state_idx
-- DROP INDEX srgis.public.ssap_state_idx;
CREATE INDEX ssap_state_idx
  ON srgis.public.ssap
  USING btree
  (btrim(upper(state::text)));

-- Index: srgis.public.ssap_strname_idx
-- DROP INDEX srgis.public.ssap_strname_idx;
CREATE INDEX ssap_strname_idx
  ON srgis.public.ssap
  USING btree
  (btrim(upper(strname::text)));

-- Index: srgis.public.ssap_posttype_idx
-- DROP INDEX srgis.public.ssap_posttype_idx;
CREATE INDEX ssap_posttype_idx
  ON srgis.public.ssap
  USING btree
  (btrim(upper(posttype::text)));

-- Index: srgis.public.ssap_zipcode_idx
-- DROP INDEX srgis.public.ssap_zipcode_idx;
CREATE INDEX ssap_zipcode_idx
  ON srgis.public.ssap
  USING btree
  (btrim(upper(zipcode::text)));

-- Index: srgis.public.roadcenterline_srcfullnam_idx
-- DROP INDEX srgis.public.roadcenterline_srcfullnam_idx;
CREATE INDEX roadcenterline_srcfullnam_idx
  ON srgis.public.roadcenterline
  USING btree
  (btrim(upper(srcfullnam::text)));

-- Index: srgis.public.roadcenterline_fromaddl_idx
-- DROP INDEX srgis.public.roadcenterline_fromaddl_idx;
CREATE INDEX roadcenterline_fromaddl_idx
  ON srgis.public.roadcenterline
  USING btree
  (btrim(upper(fromaddl::text)));

-- Index: srgis.public.roadcenterline_toaddl_idx
-- DROP INDEX srgis.public.roadcenterline_toaddl_idx;
CREATE INDEX roadcenterline_toaddl_idx
  ON srgis.public.roadcenterline
  USING btree
  (btrim(upper(toaddl::text)));

-- Index: srgis.public.roadcenterline_countryl_idx
-- DROP INDEX srgis.public.roadcenterline_countryl_idx;
CREATE INDEX roadcenterline_countryl_idx
  ON srgis.public.roadcenterline
  USING btree
  (btrim(upper(countryl::text)));

-- Index: srgis.public.roadcenterline_countyl_idx
-- DROP INDEX srgis.public.roadcenterline_countyl_idx;
CREATE INDEX roadcenterline_countyl_idx
  ON srgis.public.roadcenterline
  USING btree
  (btrim(upper(countyl::text)));

-- Index: srgis.public.roadcenterline_msagcomml_idx
-- DROP INDEX srgis.public.roadcenterline_msagcomml_idx;
CREATE INDEX roadcenterline_msagcomml_idx
  ON srgis.public.roadcenterline
  USING btree
  (btrim(upper(msagcomml::text)));

-- Index: srgis.public.roadcenterline_postcomml_idx
-- DROP INDEX srgis.public.roadcenterline_postcomml_idx;
CREATE INDEX roadcenterline_postcomml_idx
  ON srgis.public.roadcenterline
  USING btree
  (btrim(upper(postcomml::text)));

-- Index: srgis.public.roadcenterline_statel_idx
-- DROP INDEX srgis.public.roadcenterline_statel_idx;
CREATE INDEX roadcenterline_statel_idx
  ON srgis.public.roadcenterline
  USING btree
  (btrim(upper(statel::text)));

-- Index: srgis.public.roadcenterline_zipcodel_idx
-- DROP INDEX srgis.public.roadcenterline_zipcodel_idx;
CREATE INDEX roadcenterline_zipcodel_idx
  ON srgis.public.roadcenterline
  USING btree
  (btrim(upper(zipcodel::text)));

-- Index: srgis.public.roadcenterline_postdir_idx
-- DROP INDEX srgis.public.roadcenterline_postdir_idx;
CREATE INDEX roadcenterline_postdir_idx
  ON srgis.public.roadcenterline
  USING btree
  (btrim(upper(postdir::text)));

-- Index: srgis.public.roadcenterline_predir_idx
-- DROP INDEX srgis.public.roadcenterline_predir_idx;
CREATE INDEX roadcenterline_predir_idx
  ON srgis.public.roadcenterline
  USING btree
  (btrim(upper(predir::text)));

-- Index: srgis.public.roadcenterline_fromaddr_idx
-- DROP INDEX srgis.public.roadcenterline_fromaddr_idx;
CREATE INDEX roadcenterline_fromaddr_idx
  ON srgis.public.roadcenterline
  USING btree
  (btrim(upper(fromaddr::text)));

-- Index: srgis.public.roadcenterline_toaddr_idx
-- DROP INDEX srgis.public.roadcenterline_toaddr_idx;
CREATE INDEX roadcenterline_toaddr_idx
  ON srgis.public.roadcenterline
  USING btree
  (btrim(upper(toaddr::text)));

-- Index: srgis.public.roadcenterline_countryr_idx
-- DROP INDEX srgis.public.roadcenterline_countryr_idx;
CREATE INDEX roadcenterline_countryr_idx
  ON srgis.public.roadcenterline
  USING btree
  (btrim(upper(countryr::text)));

-- Index: srgis.public.roadcenterline_countyr_idx
-- DROP INDEX srgis.public.roadcenterline_countyr_idx;
CREATE INDEX roadcenterline_countyr_idx
  ON srgis.public.roadcenterline
  USING btree
  (btrim(upper(countyr::text)));

-- Index: srgis.public.roadcenterline_msagcommr_idx
-- DROP INDEX srgis.public.roadcenterline_msagcommr_idx;
CREATE INDEX roadcenterline_msagcommr_idx
  ON srgis.public.roadcenterline
  USING btree
  (btrim(upper(msagcommr::text)));

-- Index: srgis.public.roadcenterline_postcommr_idx
-- DROP INDEX srgis.public.roadcenterline_postcommr_idx;
CREATE INDEX roadcenterline_postcommr_idx
  ON srgis.public.roadcenterline
  USING btree
  (btrim(upper(postcommr::text)));

-- Index: srgis.public.roadcenterline_stater_idx
-- DROP INDEX srgis.public.roadcenterline_stater_idx;
CREATE INDEX roadcenterline_stater_idx
  ON srgis.public.roadcenterline
  USING btree
  (btrim(upper(stater::text)));

-- Index: srgis.public.roadcenterline_zipcoder_idx
-- DROP INDEX srgis.public.roadcenterline_zipcoder_idx;
CREATE INDEX roadcenterline_zipcoder_idx
  ON srgis.public.roadcenterline
  USING btree
  (btrim(upper(zipcoder::text)));

-- Index: srgis.public.roadcenterline_strname_idx
-- DROP INDEX srgis.public.roadcenterline_strname_idx;
CREATE INDEX roadcenterline_strname_idx
  ON srgis.public.roadcenterline
  USING btree
  (btrim(upper(strname::text)));

-- Index: srgis.public.roadcenterline_posttype_idx
-- DROP INDEX srgis.public.roadcenterline_posttype_idx;
CREATE INDEX roadcenterline_posttype_idx
  ON srgis.public.roadcenterline
  USING btree
  (btrim(upper(posttype::text)));"""

    return value


# End of getIndexString


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