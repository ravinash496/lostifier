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
import logging
import psycopg2 as psycopg2
from osgeo import ogr, gdal


class BulkLoader(object):

    def __init__(self, gdb_path, host, database_name, port, user_name, password, target_schema, layers_to_load):
        """
        Constructor

        :param gdb_path: Path to the input file geodatabase.
        :type gdb_path: ``str``
        :param host: The host name of the database server.
        :type host: ``str``
        :param database_name: The name of the database.
        :type database_name: ``str``
        :param port: The port the database is listening on.
        :type port: ``str``
        :param user_name: The database connection user name.
        :type user_name: ``str``
        :param password: The database connection password.
        :type password: ``str``
        :param target_schema: The Postgres schema to load to.
        :type target_schema: ``str``
        :param layers_to_load: The list of specific layers to look for and load.
        :type layers_to_load: A list of ``str``
        """
        self._gdb_path = gdb_path
        self._host = host
        self._database_name = database_name
        self._port = port
        self._user_name = user_name
        self._password = password
        self._target_schema = target_schema
        self._layers_to_load = layers_to_load
        self._connection_string = 'host={0} user={1} password={2} dbname={3} port={4}'.format(
            self._host, self._user_name, self._password, self._database_name, self._port
        )

        # Set up the base logging.
        self._logger = logging.getLogger('lostifier.bulkload.BulkLoader')
        self._logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        consolehandler = logging.StreamHandler()
        consolehandler.setLevel(logging.DEBUG)
        consolehandler.setFormatter(formatter)
        self._logger.addHandler(consolehandler)

        # Set up logging for GDAL/OGR
        gdal.PushErrorHandler(self._gdal_error_handler)

    def _gdal_error_handler(self, err_class, err_num, err_msg):
        """
        Error handler for GDAL/OGR that pipes messages out to our logs.

        :param err_class: GDAL/OGR error class
        :param err_num: GDAL/OGR error number
        :param err_msg: GDAL/OGR error message
        """
        errtype = {
            gdal.CE_None: 'None',
            gdal.CE_Debug: 'Debug',
            gdal.CE_Warning: 'Warning',
            gdal.CE_Failure: 'Failure',
            gdal.CE_Fatal: 'Fatal'
        }
        err_msg = err_msg.replace('\n', ' ')
        err_class = errtype.get(err_class, 'None')
        self._logger.error('GDAL/OGR Error: {0} : {1} : {2}'.format(err_num, err_class, err_msg))

    def _connect_postgres_db(self):
        """
        Create a connection to postgres DB

        :return: A psycopg2 connection object.
        """
        try:
            self._logger.debug('Opening psycopg2 connection to {0} on {1}.'.format(self._database_name, self._host))
            con = psycopg2.connect(self._connection_string)
        except psycopg2.Error as ex:
            self._logger.error(ex.pgerror)
            raise

        self._logger.info('Postgres (psycopg2) connection successful.')
        return con

    def _ogr_open_fgdb(self):
        """
        Opens up the file geodatabase and returns a reference.

        :return: An OGR connection to the file geodatabase.
        """
        self._logger.debug('Opening file geodatabase at {0}.'.format(self._gdb_path))
        driver = ogr.GetDriverByName('OpenFileGDB')
        gdb = driver.Open(self._gdb_path, 0)

        if gdb is None:
            self._logger.error('Unable to open file geodatabase.')
            raise Exception

        self._logger.info('File geodatabase connection successful.')
        return gdb

    def _ogr_open_postgis(self):
        """
        Opens up a connection to PostGIS

        :return: An OGR connection to PostGIS
        """
        ogr_connection_string = 'PG:{0}'.format(self._connection_string)
        self._logger.debug('Opening ogr connection to {0} on {1}.'.format(self._database_name, self._host))
        ogrds = ogr.Open(ogr_connection_string, 1)
        if ogrds is None:
            self._logger.error('Unable to open PostGIS.')
            raise Exception

        self._logger.info('Ogr connection to PostGIS successful.')
        return ogrds

    def _delete_item_from_gdb(self, gdblayer_del, name, ogrds):
        """
        Deletes an existing item from the postgres DB.

        :param gdblayer_del:
        :param name:
        :param ogrds:
        :return:
        """
        itemcount = 0
        feature = gdblayer_del.GetNextFeature()
        while feature is not None:
            gcunqid = feature.GetFieldAsString(feature.GetFieldIndex("gcUnqID"))

            self._logger.debug('Attempting to delete feature {0}.'.format(gcunqid))
            ogrds.ExecuteSQL(
                "DELETE FROM {0}.{1} WHERE gcunqid = '{2}'".format(self._target_schema, name, gcunqid), None, ''
            )
            self._logger.debug('Successfully deleted feature {0}.'.format(gcunqid))

            itemcount = itemcount + 1
            feature = gdblayer_del.GetNextFeature()

        self._logger.info('{0} items were deleted from {1}'.format(itemcount, name))

    def _verify_results(self, result, gcunqid):
        """
        Verify an error code was not thrown by CreateFeature()

        :param result:
        :param gcunqid:
        :return:
        """
        if result != 0:
            raise NameError('Process failed while trying to add item:' + gcunqid)

    def _add_item_from_gdb(self, gdblayer_add, name, ogrds):
        """
        Inserts a new item or Updates an existing item in the postgres DB.

        :param gdblayer_add:
        :param name:
        :param ogrds:
        :return:
        """
        itemcount = 0

        # Grab the next feature in the layer.
        feature = gdblayer_add.GetNextFeature()
        while feature is not None:
            # Grab the equivalent layer in PostGIS.
            postgreslayer = ogrds.GetLayerByName(name)

            # Clear FID so postgres will autogenerate next available in the sequence
            feature.SetFID(-1)
            gcunqid = feature.GetFieldAsString('gcunqid')

            self._logger.debug('Attempting to add feature {0}.'.format(gcunqid))

            # See if the feature exists already in the database.
            featureitems = ogrds.ExecuteSQL(
                "SELECT * FROM {0}.{1} where gcunqid = '{2}' ".format(self._target_schema, name, gcunqid), None, ""
            )
            fcount = featureitems.GetFeatureCount(1)

            if fcount > 0:
                # Remove item which has matching gcunqid
                ogrds.ExecuteSQL(
                    "DELETE FROM {0}.{1} where gcunqid = '{2}' ".format(self._target_schema, name, gcunqid), None, ""
                )

            # Create the new item
            result = postgreslayer.CreateFeature(feature)

            self._verify_results(result, gcunqid)
            itemcount = itemcount + 1
            feature = gdblayer_add.GetNextFeature()

        self._logger.info('{0} items were added into {1}'.format(itemcount, name))

    def _process_layer(self, name, gdb, ogrds):
        """
        Process the adds and deletes for the layer.

        :param name: The name of the layer to process.
        :param gdb: The source file geodatabase.
        :param ogrds: The destination PostGIS database.
        :return:
        """
        # Get the layer_del from the file geodatabase.
        gdblayer_del = gdb.GetLayerByName(name + '_del')

        # If the layer was found, loop though the items in the layer
        if gdblayer_del is not None:
            self._delete_item_from_gdb(gdblayer_del, name, ogrds)

        # Get the layer_add from the file geodatabase.
        gdblayer_add = gdb.GetLayerByName(name + '_add')

        # If the layer was found, loop though the items in the layer
        if gdblayer_add is not None:
            self._add_item_from_gdb(gdblayer_add, name, ogrds)

    def change_only_gdb_import(self):
        """
        Starting Location for the Change Only Process

        """
        ogrds = self._ogr_open_postgis()
        gdb = self._ogr_open_fgdb()

        # Start Transaction
        ogrds.StartTransaction()

        # For each layer name in our list of layers to load . . .
        for name in self._layers_to_load:
            self._process_layer(name, gdb, ogrds)

        for i in range(gdb.GetLayerCount()):
            layer = gdb.GetLayerByIndex(i)
            layername = layer.GetName()
            layername = str(layername)
            if layername.upper().startswith("ESB") or layername.upper().startswith("ALOC"):
                # We have found a layer which matches ESB<type> or ALOC<type>
                # Trim off _add or _delete from the layer name and pass it to _process_layer()
                trimedlayername = layername.strip('_add')
                trimedlayername = trimedlayername.strip('_del')

                self._process_layer(trimedlayername, gdb, ogrds)

        # Commit transaction
        ogrds.CommitTransaction()
        self._logger.info('All changes have been processed.')

    def full_gdb_import(self):
        """
        Process imports the full GDB overwriting any previous values.

        :return:
        """
        # Get the provisioning schema ready.
        self._reset_provisioning_schema()

        ogrds = self._ogr_open_postgis()
        gdb = self._ogr_open_fgdb()

        options = ['SCHEMA={0}'.format(self._target_schema), 'OVERWRITE=YES']

        processed_layers = []

        # For each layer name in our list of layers to load . . .
        self._logger.info('Processing standard layers . . .')
        for name in self._layers_to_load:
            # Get the layer from the file geodatabase.
            layer = gdb.GetLayerByName(name)

            # If the layer was found, copy it to the DB.
            if layer is not None:
                layername = layer.GetName()
                self._logger.info('Importing layer :: {0}'.format(layername))
                tablename = ogrds.CopyLayer(layer, name, options).GetName()
                processed_layers.append(tablename)

        self._logger.info('Processing layers starting with ESB and ALOC . . .')

        for i in range(gdb.GetLayerCount()):
            layer = gdb.GetLayerByIndex(i)
            layername = layer.GetName()
            layername = str(layername)
            if layername.upper().startswith("ESB") or layername.upper().startswith("ALOC"):
                self._logger.info('Importing layer :: {0}'.format(layername))
                tablename = ogrds.CopyLayer(layer, layername, options).GetName()
                processed_layers.append(tablename)

        self._create_primary_key(processed_layers)
        self._create_sequence(processed_layers)
        self._create_index()

    def _reset_provisioning_schema(self):
        """
        Drops and recreates the provisioning schema.

        """
        try:
            self._logger.info('Resetting target schema for bulk load provisioning . . .')
            con = self._connect_postgres_db()
            con.autocommit = True
            cursor = con.cursor()

            sqlstring = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{0}';".format(
                self._target_schema
            )
            cursor.execute(sqlstring)
            if cursor.rowcount > 0:
                sqlstring = 'DROP SCHEMA {0} CASCADE;'.format(self._target_schema)
                cursor.execute(sqlstring)

            sqlstring = 'CREATE SCHEMA {0};'.format(self._target_schema)
            cursor.execute(sqlstring)
            self._logger.info('Bulk load provisioning schema ready.')

        except psycopg2.Error as ex:
            self._logger.error(ex.pgerror)
        finally:
            con.close()

    def _create_primary_key(self, processed_layers):
        """
        Alters each table's primary key to gcunqid field

        :param processed_layers: The layers that were imported into the database.
        :type processed_layers: A list of ``str``
        """
        try:
            con = self._connect_postgres_db()
            con.autocommit = True
            cursor = con.cursor()

            for processed_layer in processed_layers:
                constraint_name = processed_layer.split('.')[1]
                sqlstring = 'ALTER TABLE {0} DROP CONSTRAINT {1}_pkey;'.format(processed_layer, constraint_name)
                cursor.execute(sqlstring)

                sqlstring = 'ALTER TABLE {0} ADD PRIMARY KEY (gcunqid)'.format(processed_layer)
                cursor.execute(sqlstring)

                self._logger.debug('Primary key has been set to gcunqid for the table {0}'.format(processed_layer))

        except psycopg2.Error as ex:
            self._logger.error(ex.pgerror)
        finally:
            con.close()

    def _create_sequence(self, processed_layers):
        """
        Update sequence values for the tables in postgres.

        :param processed_layers: The layers that were imported into the database.
        :type processed_layers: A list of ``str``
        """
        # ogrds.ExecuteSQL("") would not run the setval command so we created a new connection
        # with psycopg2 to run the SQL statement
        try:
            con = self._connect_postgres_db()
            con.autocommit = True
            cursor = con.cursor()

            for processed_layer in processed_layers:
                sqlstring = "SELECT setval(pg_get_serial_sequence('{0}', 'ogc_fid'), max(ogc_fid)) FROM {0};".format(
                    processed_layer)
                cursor.execute(sqlstring)
                self._logger.debug(
                    'Postgres primary key sequence has been reset for the table {0}'.format(processed_layer)
                )

        except psycopg2.Error as ex:
            self._logger.error(ex.pgerror)
        finally:
            con.close()

    def _create_index(self):
        """
        Apply Index to specific fields in postgres tables

        """
        try:
            con = self._connect_postgres_db()
            con.autocommit = True
            cursor = con.cursor()

            sqlstring = self._get_index_string()
            cursor.execute(sqlstring)
            self._logger.info("Index's have been applied.")

        except psycopg2.Error as ex:
            self._logger.error(ex.pgerror)
        finally:
            con.close()

    def _get_index_string(self):
        """
        Creates a string containting the full SQL command for all of the index's
        :return: string
        """

        value = """
        -- Index: srgis.{0}.ssap_srcfulladr_idx
        -- DROP INDEX srgis.{0}.ssap_srcfulladr_idx;
        CREATE INDEX ssap_srcfulladr_idx
          ON srgis.{0}.ssap
          USING btree
          (btrim(upper(srcfulladr::text)));
        
        -- Index: srgis.{0}.ssap_addnum_idx
        -- DROP INDEX srgis.{0}.ssap_addnum_idx;
        CREATE INDEX ssap_addnum_idx
          ON srgis.{0}.ssap
          USING btree
          (btrim(upper(addnum::text)));
        
        -- Index: srgis.{0}.ssap_country_idx
        -- DROP INDEX srgis.{0}.ssap_country_idx;
        CREATE INDEX ssap_country_idx
          ON srgis.{0}.ssap
          USING btree
          (btrim(upper(country::text)));
        
        -- Index: srgis.{0}.ssap_county_idx
        -- DROP INDEX srgis.{0}.ssap_county_idx;
        CREATE INDEX ssap_county_idx
          ON srgis.{0}.ssap
          USING btree
          (btrim(upper(county::text)));
        
        -- Index: srgis.{0}.ssap_msagcomm_idx
        -- DROP INDEX srgis.{0}.ssap_msagcomm_idx;
        CREATE INDEX ssap_msagcomm_idx
          ON srgis.{0}.ssap
          USING btree
          (btrim(upper(msagcomm::text)));
        
        -- Index: srgis.{0}.ssap_postcomm_idx
        -- DROP INDEX srgis.{0}.ssap_postcomm_idx_idx;
        CREATE INDEX ssap_postcomm_idx
          ON srgis.{0}.ssap
          USING btree
          (btrim(upper(postcomm::text)));
        
        -- Index: srgis.{0}.ssap_postdir_idx
        -- DROP INDEX srgis.{0}.ssap_postdir_idx;
        CREATE INDEX ssap_postdir_idx
          ON srgis.{0}.ssap
          USING btree
          (btrim(upper(postdir::text)));
        
        -- Index: srgis.{0}.ssap_predir_idx
        -- DROP INDEX srgis.{0}.ssap_predir_idx;
        CREATE INDEX ssap_predir_idx
          ON srgis.{0}.ssap
          USING btree
          (btrim(upper(predir::text)));
        
        -- Index: srgis.{0}.ssap_state_idx
        -- DROP INDEX srgis.{0}.ssap_state_idx;
        CREATE INDEX ssap_state_idx
          ON srgis.{0}.ssap
          USING btree
          (btrim(upper(state::text)));
        
        -- Index: srgis.{0}.ssap_strname_idx
        -- DROP INDEX srgis.{0}.ssap_strname_idx;
        CREATE INDEX ssap_strname_idx
          ON srgis.{0}.ssap
          USING btree
          (btrim(upper(strname::text)));
        
        -- Index: srgis.{0}.ssap_posttype_idx
        -- DROP INDEX srgis.{0}.ssap_posttype_idx;
        CREATE INDEX ssap_posttype_idx
          ON srgis.{0}.ssap
          USING btree
          (btrim(upper(posttype::text)));
        
        -- Index: srgis.{0}.ssap_zipcode_idx
        -- DROP INDEX srgis.{0}.ssap_zipcode_idx;
        CREATE INDEX ssap_zipcode_idx
          ON srgis.{0}.ssap
          USING btree
          (btrim(upper(zipcode::text)));
        
        -- Index: srgis.{0}.roadcenterline_srcfullnam_idx
        -- DROP INDEX srgis.{0}.roadcenterline_srcfullnam_idx;
        CREATE INDEX roadcenterline_srcfullnam_idx
          ON srgis.{0}.roadcenterline
          USING btree
          (btrim(upper(srcfullnam::text)));
        
        -- Index: srgis.{0}.roadcenterline_fromaddl_idx
        -- DROP INDEX srgis.{0}.roadcenterline_fromaddl_idx;
        CREATE INDEX roadcenterline_fromaddl_idx
          ON srgis.{0}.roadcenterline
          USING btree
          (btrim(upper(fromaddl::text)));
        
        -- Index: srgis.{0}.roadcenterline_toaddl_idx
        -- DROP INDEX srgis.{0}.roadcenterline_toaddl_idx;
        CREATE INDEX roadcenterline_toaddl_idx
          ON srgis.{0}.roadcenterline
          USING btree
          (btrim(upper(toaddl::text)));
        
        -- Index: srgis.{0}.roadcenterline_countryl_idx
        -- DROP INDEX srgis.{0}.roadcenterline_countryl_idx;
        CREATE INDEX roadcenterline_countryl_idx
          ON srgis.{0}.roadcenterline
          USING btree
          (btrim(upper(countryl::text)));
        
        -- Index: srgis.{0}.roadcenterline_countyl_idx
        -- DROP INDEX srgis.{0}.roadcenterline_countyl_idx;
        CREATE INDEX roadcenterline_countyl_idx
          ON srgis.{0}.roadcenterline
          USING btree
          (btrim(upper(countyl::text)));
        
        -- Index: srgis.{0}.roadcenterline_msagcomml_idx
        -- DROP INDEX srgis.{0}.roadcenterline_msagcomml_idx;
        CREATE INDEX roadcenterline_msagcomml_idx
          ON srgis.{0}.roadcenterline
          USING btree
          (btrim(upper(msagcomml::text)));
        
        -- Index: srgis.{0}.roadcenterline_postcomml_idx
        -- DROP INDEX srgis.{0}.roadcenterline_postcomml_idx;
        CREATE INDEX roadcenterline_postcomml_idx
          ON srgis.{0}.roadcenterline
          USING btree
          (btrim(upper(postcomml::text)));
        
        -- Index: srgis.{0}.roadcenterline_statel_idx
        -- DROP INDEX srgis.{0}.roadcenterline_statel_idx;
        CREATE INDEX roadcenterline_statel_idx
          ON srgis.{0}.roadcenterline
          USING btree
          (btrim(upper(statel::text)));
        
        -- Index: srgis.{0}.roadcenterline_zipcodel_idx
        -- DROP INDEX srgis.{0}.roadcenterline_zipcodel_idx;
        CREATE INDEX roadcenterline_zipcodel_idx
          ON srgis.{0}.roadcenterline
          USING btree
          (btrim(upper(zipcodel::text)));
        
        -- Index: srgis.{0}.roadcenterline_postdir_idx
        -- DROP INDEX srgis.{0}.roadcenterline_postdir_idx;
        CREATE INDEX roadcenterline_postdir_idx
          ON srgis.{0}.roadcenterline
          USING btree
          (btrim(upper(postdir::text)));
        
        -- Index: srgis.{0}.roadcenterline_predir_idx
        -- DROP INDEX srgis.{0}.roadcenterline_predir_idx;
        CREATE INDEX roadcenterline_predir_idx
          ON srgis.{0}.roadcenterline
          USING btree
          (btrim(upper(predir::text)));
        
        -- Index: srgis.{0}.roadcenterline_fromaddr_idx
        -- DROP INDEX srgis.{0}.roadcenterline_fromaddr_idx;
        CREATE INDEX roadcenterline_fromaddr_idx
          ON srgis.{0}.roadcenterline
          USING btree
          (btrim(upper(fromaddr::text)));
        
        -- Index: srgis.{0}.roadcenterline_toaddr_idx
        -- DROP INDEX srgis.{0}.roadcenterline_toaddr_idx;
        CREATE INDEX roadcenterline_toaddr_idx
          ON srgis.{0}.roadcenterline
          USING btree
          (btrim(upper(toaddr::text)));
        
        -- Index: srgis.{0}.roadcenterline_countryr_idx
        -- DROP INDEX srgis.{0}.roadcenterline_countryr_idx;
        CREATE INDEX roadcenterline_countryr_idx
          ON srgis.{0}.roadcenterline
          USING btree
          (btrim(upper(countryr::text)));
        
        -- Index: srgis.{0}.roadcenterline_countyr_idx
        -- DROP INDEX srgis.{0}.roadcenterline_countyr_idx;
        CREATE INDEX roadcenterline_countyr_idx
          ON srgis.{0}.roadcenterline
          USING btree
          (btrim(upper(countyr::text)));
        
        -- Index: srgis.{0}.roadcenterline_msagcommr_idx
        -- DROP INDEX srgis.{0}.roadcenterline_msagcommr_idx;
        CREATE INDEX roadcenterline_msagcommr_idx
          ON srgis.{0}.roadcenterline
          USING btree
          (btrim(upper(msagcommr::text)));
        
        -- Index: srgis.{0}.roadcenterline_postcommr_idx
        -- DROP INDEX srgis.{0}.roadcenterline_postcommr_idx;
        CREATE INDEX roadcenterline_postcommr_idx
          ON srgis.{0}.roadcenterline
          USING btree
          (btrim(upper(postcommr::text)));
        
        -- Index: srgis.{0}.roadcenterline_stater_idx
        -- DROP INDEX srgis.{0}.roadcenterline_stater_idx;
        CREATE INDEX roadcenterline_stater_idx
          ON srgis.{0}.roadcenterline
          USING btree
          (btrim(upper(stater::text)));
        
        -- Index: srgis.{0}.roadcenterline_zipcoder_idx
        -- DROP INDEX srgis.{0}.roadcenterline_zipcoder_idx;
        CREATE INDEX roadcenterline_zipcoder_idx
          ON srgis.{0}.roadcenterline
          USING btree
          (btrim(upper(zipcoder::text)));
        
        -- Index: srgis.{0}.roadcenterline_strname_idx
        -- DROP INDEX srgis.{0}.roadcenterline_strname_idx;
        CREATE INDEX roadcenterline_strname_idx
          ON srgis.{0}.roadcenterline
          USING btree
          (btrim(upper(strname::text)));
        
        -- Index: srgis.{0}.roadcenterline_posttype_idx
        -- DROP INDEX srgis.{0}.roadcenterline_posttype_idx;
        CREATE INDEX roadcenterline_posttype_idx
          ON srgis.{0}.roadcenterline
          USING btree
          (btrim(upper(posttype::text)));""".format(self._target_schema)

        return value


if __name__ == "__main__":

    # The list of layers we want to load.
    layers_to_load = [
        'CountyBoundary', 'UnIncCommBoundary', 'IncMunicipalBoundary', 'StateBoundary', 'RoadCenterline', 'SSAP'
    ]

    gdb_path = input("Please enter the complete source path for gdb file:")
    script_mode = input("Select bulk import mode. Type 'C' for change only or type 'F' for a full gbd import.")
    db_host = input("Enter the host name of the database:")
    db_name = input("Enter the name of the database:")
    db_port = input("Enter the database port:")
    db_user = input("Enter the database user name:")
    db_password = input("Enter the database password:")
    db_target_schema = input("Enter the name of the provisioning schema:")

    bulkloader = BulkLoader(gdb_path, db_host, db_name, db_port, db_user, db_password, db_target_schema, layers_to_load)

    try:
        if script_mode.lower() == 'c':
            bulkloader.change_only_gdb_import()
        elif script_mode.lower() == 'f':
            bulkloader.full_gdb_import()
        else:
            print("Invalid option detected, goodbye")
    except NameError:
        print('An error was encountered and the process has been terminated.')
        raise
