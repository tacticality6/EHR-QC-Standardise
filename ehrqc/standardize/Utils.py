import psycopg2

from ehrqc.standardize import Config

import logging

log = logging.getLogger("EHR-QC")


def getConnection():

    # Connect to postgres with a copy of the MIMIC-III database
    con = psycopg2.connect(
        dbname=Config.db_details["sql_db_name"],
        user=Config.db_details["sql_user_name"],
        host=Config.db_details["sql_host_name"],
        port=Config.db_details["sql_port_number"],
        password=Config.db_details["sql_password"]
        )

    return con


def saveDataframe(con, destinationSchemaName, destinationTableName, columns, df, dfColumns):

    import numpy as np
    import psycopg2.extras
    import psycopg2.extensions

    psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

    log.info("Importing data to table: " + destinationSchemaName + '.' + destinationTableName)

    if len(df) > 0:
        table = destinationSchemaName + '.' + destinationTableName
        values = "VALUES({})".format(",".join(["%s" for _ in dfColumns]))
        columnsString = '"' + '", "'.join(columns) + '"'
        insert_stmt = "INSERT INTO {} ({}) {}".format(table, columnsString, values)
        try:
            cur = con.cursor()
            psycopg2.extras.execute_batch(cur, insert_stmt, df[dfColumns].values)
            con.commit()
        finally:
            cur.close()


def createSchema(con, schemaName):
    log.info("Creating schema: " + schemaName)
    createSchemaQuery = """create schema if not exists """ + schemaName
    with con:
        with con.cursor() as cursor:
            cursor.execute(createSchemaQuery)
