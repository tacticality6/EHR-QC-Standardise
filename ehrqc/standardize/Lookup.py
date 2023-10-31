from ehrqc.standardize import Config
from ehrqc.standardize import Utils

import logging

log = logging.getLogger("EHR-QC")


def createConcept(con, etlSchemaName, filePath):
    log.info("Creating table: " + etlSchemaName + ".voc_concept")
    dropQuery = """drop table if exists """ + etlSchemaName + """.voc_concept cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.voc_concept (
        concept_id          INTEGER       not null ,
        concept_name        TEXT      ,
        domain_id           TEXT      not null ,
        vocabulary_id       TEXT      not null ,
        concept_class_id    TEXT      not null ,
        standard_concept    TEXT               ,
        concept_code        TEXT      not null ,
        valid_start_date    DATE        not null ,
        valid_end_date      DATE        not null ,
        invalid_reason      TEXT
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd

    df = pd.read_csv(filePath, sep='\t')
    df['valid_start_date'] = pd.to_datetime(df['valid_start_date'], unit='s')
    df['valid_end_date'] = pd.to_datetime(df['valid_end_date'], unit='s')
    dfColumns = ['concept_id', 'concept_name', 'domain_id', 'vocabulary_id', 'concept_class_id', 'standard_concept', 'concept_code', 'valid_start_date', 'valid_end_date', 'invalid_reason']
    Utils.saveDataframe(con=con, destinationSchemaName=etlSchemaName, destinationTableName='voc_concept', df=df, dfColumns=dfColumns)


def createVocabulary(con, etlSchemaName, filePath):
    log.info("Creating table: " + etlSchemaName + ".voc_vocabulary")
    dropQuery = """drop table if exists """ + etlSchemaName + """.voc_vocabulary cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.voc_vocabulary (
        vocabulary_id         TEXT      not null,
        vocabulary_name       TEXT      not null,
        vocabulary_reference  TEXT      not null,
        vocabulary_version    TEXT              ,
        vocabulary_concept_id INTEGER       not null
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd

    df = pd.read_csv(filePath, sep='\t')
    dfColumns = ['vocabulary_id', 'vocabulary_name', 'vocabulary_reference', 'vocabulary_version', 'vocabulary_concept_id']
    Utils.saveDataframe(con=con, destinationSchemaName=etlSchemaName, destinationTableName='voc_vocabulary', df=df, dfColumns=dfColumns)


def createDomain(con, etlSchemaName, filePath):
    log.info("Creating table: " + etlSchemaName + ".voc_domain")
    dropQuery = """drop table if exists """ + etlSchemaName + """.voc_domain cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.voc_domain (
        domain_id         TEXT      not null,
        domain_name       TEXT      not null,
        domain_concept_id INTEGER       not null
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd

    df = pd.read_csv(filePath, sep='\t')
    dfColumns = ['domain_id', 'domain_name', 'domain_concept_id']
    Utils.saveDataframe(con=con, destinationSchemaName=etlSchemaName, destinationTableName='voc_domain', df=df, dfColumns=dfColumns)


def createConceptClass(con, etlSchemaName, filePath):
    log.info("Creating table: " + etlSchemaName + ".voc_concept_class")
    dropQuery = """drop table if exists """ + etlSchemaName + """.voc_concept_class cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.voc_concept_class (
        concept_class_id          TEXT      not null,
        concept_class_name        TEXT      not null,
        concept_class_concept_id  INTEGER       not null
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd

    df = pd.read_csv(filePath, sep='\t')
    dfColumns = ['concept_class_id', 'concept_class_name', 'concept_class_concept_id']
    Utils.saveDataframe(con=con, destinationSchemaName=etlSchemaName, destinationTableName='voc_concept_class', df=df, dfColumns=dfColumns)


def createConceptRelationship(con, etlSchemaName, filePath):
    log.info("Creating table: " + etlSchemaName + ".voc_concept_relationship")
    dropQuery = """drop table if exists """ + etlSchemaName + """.voc_concept_relationship cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.voc_concept_relationship (
        concept_id_1      INTEGER     not null,
        concept_id_2      INTEGER     not null,
        relationship_id   TEXT    not null,
        valid_start_DATE  DATE      not null,
        valid_end_DATE    DATE      not null,
        invalid_reason    TEXT
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd

    df = pd.read_csv(filePath, sep='\t')
    df['valid_start_date'] = pd.to_datetime(df['valid_start_date'], unit='s')
    df['valid_end_date'] = pd.to_datetime(df['valid_end_date'], unit='s')
    dfColumns = ['concept_id_1', 'concept_id_2', 'relationship_id', 'valid_start_date', 'valid_end_date', 'invalid_reason']
    Utils.saveDataframe(con=con, destinationSchemaName=etlSchemaName, destinationTableName='voc_concept_relationship', df=df, dfColumns=dfColumns)


def createRelationship(con, etlSchemaName, filePath):
    log.info("Creating table: " + etlSchemaName + ".voc_relationship")
    dropQuery = """drop table if exists """ + etlSchemaName + """.voc_relationship cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.voc_relationship (
        relationship_id         TEXT      not null,
        relationship_name       TEXT      not null,
        is_hierarchical         TEXT      not null,
        defines_ancestry        TEXT      not null,
        reverse_relationship_id TEXT      not null,
        relationship_concept_id INTEGER       not null
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd

    df = pd.read_csv(filePath, sep='\t')
    dfColumns = ['relationship_id', 'relationship_name', 'is_hierarchical', 'defines_ancestry', 'reverse_relationship_id', 'relationship_concept_id']
    Utils.saveDataframe(con=con, destinationSchemaName=etlSchemaName, destinationTableName='voc_relationship', df=df, dfColumns=dfColumns)


def createConceptSynonym(con, etlSchemaName, filePath):
    log.info("Creating table: " + etlSchemaName + ".voc_concept_synonym")
    dropQuery = """drop table if exists """ + etlSchemaName + """.voc_concept_synonym cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.voc_concept_synonym (
        concept_id            INTEGER       not null,
        concept_synonym_name  TEXT      not null,
        language_concept_id   INTEGER       not null
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd

    df = pd.read_csv(filePath, sep='\t')
    dfColumns = ['concept_id', 'concept_synonym_name', 'language_concept_id']
    Utils.saveDataframe(con=con, destinationSchemaName=etlSchemaName, destinationTableName='voc_concept_synonym', df=df, dfColumns=dfColumns)


def createConceptAncestor(con, etlSchemaName, filePath):
    log.info("Creating table: " + etlSchemaName + ".voc_concept_ancestor")
    dropQuery = """drop table if exists """ + etlSchemaName + """.voc_concept_ancestor cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.voc_concept_ancestor (
        ancestor_concept_id       INTEGER   not null,
        descendant_concept_id     INTEGER   not null,
        min_levels_of_separation  INTEGER   not null,
        max_levels_of_separation  INTEGER   not null
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd

    df = pd.read_csv(filePath, sep='\t')
    dfColumns = ['ancestor_concept_id', 'descendant_concept_id', 'min_levels_of_separation', 'max_levels_of_separation']
    Utils.saveDataframe(con=con, destinationSchemaName=etlSchemaName, destinationTableName='voc_concept_ancestor', df=df, dfColumns=dfColumns)


def stageConcept(con, etlSchemaName, lookupSchemaName):
    log.info("Creating table: " + lookupSchemaName + ".concept")
    dropQuery = """drop table if exists """ + lookupSchemaName + """.concept cascade"""
    createQuery = """CREATE TABLE """ + lookupSchemaName + """.concept AS
        SELECT
            *,
            'concept' AS load_table_id,
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int AS load_row_id
        FROM
            """ + etlSchemaName + """.voc_concept
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def stageConceptRelationship(con, etlSchemaName, lookupSchemaName):
    log.info("Creating table: " + lookupSchemaName + ".concept_relationship")
    dropQuery = """drop table if exists """ + lookupSchemaName + """.concept_relationship cascade"""
    createQuery = """CREATE TABLE """ + lookupSchemaName + """.concept_relationship AS
        SELECT
            *,
            'concept_relationship' AS load_table_id,
            0 AS load_row_id
        FROM
            """ + etlSchemaName + """.voc_concept_relationship
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def stageVocabulary(con, etlSchemaName, lookupSchemaName):
    log.info("Creating table: " + lookupSchemaName + ".vocabulary")
    dropQuery = """drop table if exists """ + lookupSchemaName + """.vocabulary cascade"""
    createQuery = """CREATE  TABLE """ + lookupSchemaName + """.vocabulary AS
        SELECT
            *,
            'vocabulary' AS load_table_id,
            0 AS load_row_id
        FROM
            """ + etlSchemaName + """.voc_vocabulary
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def stageConceptClass(con, etlSchemaName, lookupSchemaName):
    log.info("Creating table: " + lookupSchemaName + ".concept_class")
    dropQuery = """drop table if exists """ + lookupSchemaName + """.concept_class cascade"""
    createQuery = """CREATE TABLE """ + lookupSchemaName + """.concept_class AS
        SELECT
            *,
            'concept_class' AS load_table_id,
            0 AS load_row_id
        FROM
            """ + etlSchemaName + """.voc_concept_class
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def stageConceptAncestor(con, etlSchemaName, lookupSchemaName):
    log.info("Creating table: " + lookupSchemaName + ".concept_ancestor")
    dropQuery = """drop table if exists """ + lookupSchemaName + """.concept_ancestor cascade"""
    createQuery = """CREATE TABLE """ + lookupSchemaName + """.concept_ancestor AS
        SELECT
            *,
            'concept_ancestor' AS load_table_id,
            0 AS load_row_id
        FROM
            """ + etlSchemaName + """.voc_concept_ancestor
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def stageConceptSynonym(con, etlSchemaName, lookupSchemaName):
    log.info("Creating table: " + lookupSchemaName + ".concept_synonym")
    dropQuery = """drop table if exists """ + lookupSchemaName + """.concept_synonym cascade"""
    createQuery = """CREATE TABLE """ + lookupSchemaName + """.concept_synonym AS
        SELECT
            *,
            'concept_synonym' AS load_table_id,
            0 AS load_row_id
        FROM
            """ + etlSchemaName + """.voc_concept_synonym
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def stageDomain(con, etlSchemaName, lookupSchemaName):
    log.info("Creating table: " + lookupSchemaName + ".domain")
    dropQuery = """drop table if exists """ + lookupSchemaName + """.domain cascade"""
    createQuery = """CREATE  TABLE """ + lookupSchemaName + """.domain AS
        SELECT
            *,
            'domain' AS load_table_id,
            0 AS load_row_id
        FROM
            """ + etlSchemaName + """.voc_domain
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def stageRelationship(con, etlSchemaName, lookupSchemaName):
    log.info("Creating table: " + lookupSchemaName + ".relationship")
    dropQuery = """drop table if exists """ + lookupSchemaName + """.relationship cascade"""
    createQuery = """CREATE TABLE """ + lookupSchemaName + """.relationship AS
        SELECT
            *,
            'relationship' AS load_table_id,
            0 AS load_row_id
        FROM
            """ + etlSchemaName + """.voc_relationship
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def importConceptMapping(con, schemaName, filePath, fileSeparator):
    log.info("Creating table: " + schemaName + ".concept_mapping")
    dropQuery = """drop table if exists """ + schemaName + """.concept_mapping cascade"""
    createQuery = """CREATE TABLE """ + schemaName + """.concept_mapping (
        concept_name         TEXT      not null,
        snomed_concept_id       TEXT      not null
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd

    df = pd.read_csv(filePath, sep=fileSeparator)
    dfColumns = []
    columns = []
    if(Config.conceptmaps['column_mapping']['concept_name']):
        dfColumns.append(Config.conceptmaps['column_mapping']['concept_name'])
        columns.append('concept_name')
    if(Config.conceptmaps['column_mapping']['snomed_concept_id']):
        dfColumns.append(Config.conceptmaps['column_mapping']['snomed_concept_id'])
        columns.append('snomed_concept_id')
    Utils.saveDataframe(con=con, destinationSchemaName=schemaName, destinationTableName='concept_mapping', columns=columns, df=df, dfColumns=dfColumns)


def importAthenaVocabulary(con):
    Utils.createSchema(con=con, schemaName=Config.etl_schema_name)
    if(Config.vocabulary['concept']):
        createConcept(con=con, etlSchemaName=Config.etl_schema_name, filePath = Config.vocabulary['concept'])
    if(Config.vocabulary['concept']):
        createVocabulary(con=con, etlSchemaName=Config.etl_schema_name, filePath = Config.vocabulary['vocabulary'])
    if(Config.vocabulary['concept']):
        createDomain(con=con, etlSchemaName=Config.etl_schema_name, filePath = Config.vocabulary['domain'])
    if(Config.vocabulary['concept']):
        createConceptClass(con=con, etlSchemaName=Config.etl_schema_name, filePath = Config.vocabulary['concept_class'])
    if(Config.vocabulary['concept']):
        createConceptRelationship(con=con, etlSchemaName=Config.etl_schema_name, filePath = Config.vocabulary['concept_relationship'])
    if(Config.vocabulary['concept']):
        createRelationship(con=con, etlSchemaName=Config.etl_schema_name, filePath = Config.vocabulary['relationship'])
    if(Config.vocabulary['concept']):
        createConceptSynonym(con=con, etlSchemaName=Config.etl_schema_name, filePath = Config.vocabulary['concept_synonym'])
    if(Config.vocabulary['concept']):
        createConceptAncestor(con=con, etlSchemaName=Config.etl_schema_name, filePath = Config.vocabulary['concept_ancestor'])


def stageAthenaVocabulary(con):
    Utils.createSchema(con=con, schemaName=Config.lookup_schema_name)
    stageConcept(con=con, etlSchemaName=Config.etl_schema_name, lookupSchemaName=Config.lookup_schema_name)
    stageConceptRelationship(con=con, etlSchemaName=Config.etl_schema_name, lookupSchemaName=Config.lookup_schema_name)
    stageVocabulary(con=con, etlSchemaName=Config.etl_schema_name, lookupSchemaName=Config.lookup_schema_name)
    stageConceptClass(con=con, etlSchemaName=Config.etl_schema_name, lookupSchemaName=Config.lookup_schema_name)
    stageConceptAncestor(con=con, etlSchemaName=Config.etl_schema_name, lookupSchemaName=Config.lookup_schema_name)
    stageConceptSynonym(con=con, etlSchemaName=Config.etl_schema_name, lookupSchemaName=Config.lookup_schema_name)
    stageDomain(con=con, etlSchemaName=Config.etl_schema_name, lookupSchemaName=Config.lookup_schema_name)
    stageRelationship(con=con, etlSchemaName=Config.etl_schema_name, lookupSchemaName=Config.lookup_schema_name)


def migrateStandardVocabulary(con):
    importAthenaVocabulary(con=con)
    stageAthenaVocabulary(con=con)


# if __name__ == "__main__":
#     import logging
#     import sys

#     log = logging.getLogger("EHR-QC")
#     log.setLevel(logging.INFO)
#     format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

#     ch = logging.StreamHandler(sys.stdout)
#     ch.setFormatter(format)
#     log.addHandler(ch)

#     log.info('Getting DB connection')
#     con = Utils.getConnection()
#     log.info('Migrating standard vocabulary')
#     migrateStandardVocabulary(con=con)
