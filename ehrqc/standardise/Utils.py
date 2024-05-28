import psycopg2

import logging

log = logging.getLogger("EHR-QC")

# TODO
# Get DB values from env or something more
import os

# os.environ["POSTGRES_HOSTNAME"] = "localhost"
os.environ["POSTGRES_HOSTNAME"] = "db"
os.environ["POSTGRES_PORT_NUMBER"] = "5432"
os.environ["POSTGRES_USER_NAME"] = "postgres"
os.environ["POSTGRES_PASSWORD"] = "mypassword"
os.environ["POSTGRES_DB_NAME"] = "postgres"


def getConnection(Config):

    con = psycopg2.connect(
        dbname=os.environ.get("POSTGRES_DB_NAME"),
        user=os.environ.get("POSTGRES_USER_NAME"),
        host=os.environ.get("POSTGRES_HOSTNAME"),
        port=os.environ.get("POSTGRES_PORT_NUMBER"),
        password=os.environ.get("POSTGRES_PASSWORD"),
    )

    return con


def saveDataframe(
    con, destinationSchemaName, destinationTableName, columns, df, dfColumns
):

    import numpy as np
    import psycopg2.extras
    import psycopg2.extensions

    psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

    log.info(
        "Importing data to table: " + destinationSchemaName + "." + destinationTableName
    )

    if len(df) > 0:
        table = destinationSchemaName + "." + destinationTableName
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
