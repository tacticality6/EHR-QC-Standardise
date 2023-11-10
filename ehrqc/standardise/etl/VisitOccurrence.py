import logging

log = logging.getLogger("EHR-QC")


def createVisitOccurrenceCdm(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".cdm_visit_occurrence")
    dropQuery = """drop table if exists """ + etlSchemaName + """.cdm_visit_occurrence cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.cdm_visit_occurrence
        (
            visit_occurrence_id           INTEGER     not null ,
            person_id                     INTEGER     not null ,
            visit_concept_id              INTEGER              ,
            visit_start_date              DATE                 ,
            visit_start_datetime          TIMESTAMP            ,
            visit_end_date                DATE                 ,
            visit_end_datetime            TIMESTAMP            ,
            visit_type_concept_id         INTEGER              ,
            provider_id                   INTEGER              ,
            care_site_id                  INTEGER              ,
            visit_source_value            TEXT                 ,
            visit_source_concept_id       INTEGER              ,
            admitting_source_concept_id   INTEGER              ,
            admitting_source_value        TEXT                 ,
            discharge_to_concept_id       INTEGER              ,
            discharge_to_source_value     TEXT                 ,
            preceding_visit_occurrence_id INTEGER              ,
            unit_id                       TEXT,
            load_table_id                 TEXT,
            load_row_id                   INTEGER,
            trace_id                      TEXT
        )
        ;
        """
    insertQuery = """INSERT INTO """ + etlSchemaName + """.cdm_visit_occurrence
    SELECT
        src.episode_id::int                         AS visit_occurrence_id,
        replace(src.patient_id, '-', '')::int       AS person_id,
        0                                           AS visit_concept_id,
        CAST(src.admittime AS DATE)                 AS visit_start_date,
        src.admittime                               AS visit_start_datetime,
        CAST(src.dischtime AS DATE)                 AS visit_end_date,
        src.dischtime                               AS visit_end_datetime,
        32817                                       AS visit_type_concept_id,
        CAST(NULL AS INTEGER)                       AS provider_id,
        CAST(NULL AS INTEGER)                       AS care_site_id,
        src.admission_type                          AS visit_source_value,
        CAST(NULL AS INTEGER)                       AS visit_source_concept_id,
        0                                           AS admitting_source_concept_id,
        src.admission_location                      AS admitting_source_value,
        0                                           AS discharge_to_concept_id,         
        src.discharge_location                      AS discharge_to_source_value,
        LAG(src.episode_id) OVER (
            PARTITION BY src.patient_id, src.episode_id
            ORDER BY src.admittime
        )::int                                      AS preceding_visit_occurrence_id,
        CONCAT('visit.', src.admission_type)        AS unit_id,
        src.load_table_id                           AS load_table_id,
        src.load_row_id                             AS load_row_id,
        src.trace_id                                AS trace_id
    FROM
        """ + etlSchemaName + """.src_admissions src
    ;
        """
    with con:
        with con.cursor() as cursor:
            log.info("Dropping table if exists: " + etlSchemaName + ".cdm_visit_occurrence")
            cursor.execute(dropQuery)
            log.info("Creating table: " + etlSchemaName + ".cdm_visit_occurrence")
            cursor.execute(createQuery)
            log.info("Loading table: " + etlSchemaName + ".cdm_visit_occurrence")
            cursor.execute(insertQuery)


def migrate(con, etlSchemaName):
    createVisitOccurrenceCdm(con = con, etlSchemaName = etlSchemaName)
