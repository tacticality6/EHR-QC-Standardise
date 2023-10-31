import logging
import sys

log = logging.getLogger("EHR-QC")
log.setLevel(logging.INFO)
format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(format)
log.addHandler(ch)

import argparse

from ehrqc.standardize import Utils
from ehrqc.standardize import Config
from ehrqc.standardize import Import
from ehrqc.standardize import Stage
from ehrqc.standardize import Lookup
from ehrqc.standardize.etl import Person
from ehrqc.standardize.etl import VisitOccurrence
from ehrqc.standardize.etl import Measurement


def importVocabulary(con):
    log.info("Importing Standard Vocabulary (Athena) from CSV files")
    Lookup.migrateStandardVocabulary(con=con)


def importCsv(con):
    log.info("Importing EHR data from CSV files")
    Import.importDataCsv(con=con, sourceSchemaName=Config.source_schema_name)


def stageData(con):
    log.info("Staging EHR data")
    Stage.migrate(con=con, sourceSchemaName=Config.source_schema_name, destinationSchemaName=Config.etl_schema_name)


def importConceptMapping(con):
    log.info("Import Concept Mappings")
    Lookup.importConceptMapping(
        con=con,
        schemaName=Config.lookup_schema_name,
        filePath = Config.conceptmaps['file_name'],
        fileSeparator=Config.conceptmaps['file_separator']
        )


def performETL(con):
    log.info("Performing ETL")
    log.info("ETL for the entity: Person")
    Person.migrate(con=con, etlSchemaName=Config.etl_schema_name)
    log.info("ETL for the entity: Visit Occurrence")
    VisitOccurrence.migrate(con=con, etlSchemaName=Config.etl_schema_name)
    # log.info("ETL for the entity: Measurements")
    Measurement.migrate(con=con, etlSchemaName=Config.etl_schema_name)


if __name__ == "__main__":

    log.info("Parsing command line arguments")

    parser = argparse.ArgumentParser(description='Migrate EHR to OMOP-CDM')
    parser.add_argument('-l', '--create_lookup', action='store_true',
                        help='Create lookup by importing Athena vocabulary and custom mapping')
    parser.add_argument('-f', '--import_file', action='store_true',
                        help='Import EHR from a csv files')
    parser.add_argument('-s', '--stage', action='store_true',
                        help='Stage the data on the ETL schema')
    parser.add_argument('-c', '--import_concept_mapping', action='store_true',
                        help='Import concept mapping file')
    parser.add_argument('-e', '--perform_etl', action='store_true',
                        help='Perform migration Extract-Transform-Load (ETL) operations')
    parser.add_argument('-u', '--unload', action='store_true',
                        help='Unload data to CDM schema')

    args = parser.parse_args()

    log.info("Start!!")

    con = Utils.getConnection()

    if args.create_lookup:
        Utils.createSchema(con=con, schemaName=Config.lookup_schema_name)

    if args.import_file:
        Utils.createSchema(con=con, schemaName=Config.source_schema_name)

    if args.create_lookup or args.import_file or args.stage or args.import_concept_mapping or args.perform_etl:
        Utils.createSchema(con=con, schemaName=Config.etl_schema_name)

    if args.unload:
        Utils.createSchema(con=con, schemaName=Config.cdm_schema_name)

    if args.create_lookup:
        importVocabulary(con=con)

    if args.import_file:
        importCsv(con=con)

    if args.stage:
        stageData(con=con)

    if args.import_concept_mapping:
        importConceptMapping(con=con)

    if args.perform_etl:
        performETL(con=con)

    # if args.unload:
    #     unloadData(con=con)

    log.info("End!!")
