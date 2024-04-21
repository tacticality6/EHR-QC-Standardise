import logging
import sys

log = logging.getLogger("EHR-QC")
log.setLevel(logging.INFO)
format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(format)
log.addHandler(ch)

import argparse


def importVocabulary(con, Config):
    from ehrqc.standardise import Lookup
    log.info("Importing Standard Vocabulary (Athena) from CSV files")
    Lookup.migrateStandardVocabulary(con=con, Config=Config)


def importCsv(con, Config):
    from ehrqc.standardise import Import

    log.info("Importing EHR data from CSV files")
    Import.importDataCsv(con=con, Config=Config)


def stageData(con, Config):
    from ehrqc.standardise import Stage
    log.info("Staging EHR data")
    Stage.migrate(
        con=con,
        sourceSchemaName=Config.source_schema_name,
        destinationSchemaName=Config.etl_schema_name,
        Config=Config
    )


def importConceptMapping(con, Config):
    from ehrqc.standardise import Lookup

    log.info("Import Concept Mappings")
    Lookup.importConceptMapping(
        con=con,
        schemaName=Config.lookup_schema_name,
        filePath=Config.conceptmaps["file_name"],
        fileSeparator=Config.conceptmaps["file_separator"],
        Config=Config
    )


def performETL(con, Config):
    from ehrqc.standardise.etl import Person
    from ehrqc.standardise.etl import VisitOccurrence
    from ehrqc.standardise.etl import Measurement

    log.info("Performing ETL")
    log.info("ETL for the entity: Person")
    Person.migrate(con=con, etlSchemaName=Config.etl_schema_name)
    log.info("ETL for the entity: Visit Occurrence")
    VisitOccurrence.migrate(con=con, etlSchemaName=Config.etl_schema_name, Config=Config)
    log.info("ETL for the entity: Measurements")
    Measurement.migrate(con=con, etlSchemaName=Config.etl_schema_name, Config=Config)


if __name__ == "__main__":

    from ehrqc.standardise import Utils

    log.info("Parsing command line arguments")

    parser = argparse.ArgumentParser(description="Migrate EHR to OMOP-CDM")
    parser.add_argument(
        "-l",
        "--create_lookup",
        action="store_true",
        help="Create lookup by importing Athena vocabulary and custom mapping",
    )
    parser.add_argument(
        "-f", "--import_file", action="store_true", help="Import EHR from a csv files"
    )
    parser.add_argument(
        "-s", "--stage", action="store_true", help="Stage the data on the ETL schema"
    )
    parser.add_argument(
        "-c",
        "--import_concept_mapping",
        action="store_true",
        help="Import concept mapping file",
    )
    parser.add_argument(
        "-e",
        "--perform_etl",
        action="store_true",
        help="Perform migration Extract-Transform-Load (ETL) operations",
    )
    parser.add_argument(
        "-u", "--unload", action="store_true", help="Unload data to CDM schema"
    )
    parser.add_argument(
        "-cf", "--config", action="store", dest="config", help="Configuration file",
    )

    args = parser.parse_args()

    import os
    import importlib.util

    # if user supplies config path, import the config file as a module
    # else use the default config.py
    if args.config:
        # Get the absolute path of the parent directory (3 levels up)
        parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))

        # Construct the file path to the config file in the parent directory
        config_file_path = os.path.join(parent_dir, args.config)

        spec = importlib.util.spec_from_file_location("Config", config_file_path)
        config_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(config_module)
        Config = config_module
    else:
        from ehrqc.standardise import Config

    con = Utils.getConnection(Config=Config)

    if args.create_lookup:
        Utils.createSchema(con=con, schemaName=Config.lookup_schema_name)

    if args.import_file:
        Utils.createSchema(con=con, schemaName=Config.source_schema_name)

    if (
        args.create_lookup
        or args.import_file
        or args.stage
        or args.import_concept_mapping
        or args.perform_etl
    ):
        Utils.createSchema(con=con, schemaName=Config.etl_schema_name)

    if args.unload:
        Utils.createSchema(con=con, schemaName=Config.cdm_schema_name)

    if args.create_lookup:
        importVocabulary(con=con, Config=Config)

    if args.import_file:
        importCsv(con=con, Config=Config)

    if args.stage:
        stageData(con=con, Config=Config)

    if args.import_concept_mapping:
        importConceptMapping(con=con, Config=Config)

    if args.perform_etl:
        performETL(con=con, Config=Config)

    # if args.unload:
    #     unloadData(con=con, Config=Config)

    log.info("End!!")
