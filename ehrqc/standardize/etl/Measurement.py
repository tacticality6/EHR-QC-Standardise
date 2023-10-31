import logging

log = logging.getLogger("EHR-QC")


def createMeasurements(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".cdm_measurement")
    dropQuery = """drop table if exists """ + etlSchemaName + """.cdm_measurement cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.cdm_measurement
        (
            measurement_id                INTEGER     not null ,
            person_id                     INTEGER     not null ,
            measurement_concept_id        TEXT        not null ,
            measurement_date              DATE                 ,
            measurement_datetime          TIMESTAMP            ,
            measurement_time              TEXT                 ,
            measurement_type_concept_id   INTEGER              ,
            operator_concept_id           INTEGER              ,
            value_as_number               FLOAT                ,
            value_as_concept_id           INTEGER              ,
            unit_concept_id               INTEGER              ,
            range_low                     TEXT                 ,
            range_high                    TEXT                 ,
            provider_id                   INTEGER              ,
            visit_occurrence_id           INTEGER              ,
            visit_detail_id               INTEGER              ,
            measurement_source_value      TEXT                 ,
            measurement_source_concept_id INTEGER              ,
            unit_source_value             TEXT                 ,
            value_source_value            TEXT                 ,
            unit_id                       TEXT,
            load_table_id                 TEXT,
            load_row_id                   INTEGER,
            trace_id                      TEXT  
        )
        ;
        """
    insertLabeventsQuery = """INSERT INTO """ + etlSchemaName + """.cdm_measurement
        SELECT
            src.labevent_id                                     AS measurement_id,
            src.patient_id                                      AS person_id,
            map.snomed_concept_id::TEXT                         AS measurement_concept_id,
            CAST(src.charttime AS DATE)                         AS measurement_date,
            src.charttime                                       AS measurement_datetime,
            CAST(src.charttime AS TIME)                         AS measurement_time,
            32856                                               AS measurement_type_concept_id,
            CAST(NULL AS INTEGER)                               AS operator_concept_id,
            CAST(src.valuenum AS FLOAT)                         AS value_as_number,
            CAST(NULL AS INTEGER)                               AS value_as_concept_id,
            CAST(NULL AS INTEGER)                               AS unit_concept_id,
            src.ref_range_lower                                 AS range_low,
            src.ref_range_upper                                 AS range_high,
            CAST(NULL AS INTEGER)                               AS provider_id,
            src.episode_id                                      AS visit_occurrence_id,
            CAST(NULL AS INTEGER)                               AS visit_detail_id,
            src.itemid                                          AS measurement_source_value,
            CAST(NULL AS INTEGER)                               AS measurement_source_concept_id,
            src.valueuom                                        AS unit_source_value,
            src.value                                           AS value_source_value,
            'labevents'                                         AS unit_id,
            src.load_table_id                                   AS load_table_id,
            src.load_row_id                                     AS load_row_id,
            src.trace_id                                        AS trace_id
        FROM  
            """ + etlSchemaName + """.src_labevents src
        INNER JOIN """ + etlSchemaName + """.concept_mapping map
        ON map.concept_name = src.itemid
        WHERE src.value ~ '^[0-9\.]+$'
        ;
        """
    insertCharteventsQuery = """INSERT INTO """ + etlSchemaName + """.cdm_measurement
        SELECT
            src.vital_id                                        AS measurement_id,
            src.patient_id                                      AS person_id,
            map.snomed_concept_id::TEXT                         AS measurement_concept_id,
            CAST(src.charttime AS DATE)                         AS measurement_date,
            src.charttime                                       AS measurement_datetime,
            CAST(src.charttime AS TIME)                         AS measurement_time,
            CAST(NULL AS INTEGER)                               AS measurement_type_concept_id,
            CAST(NULL AS INTEGER)                               AS operator_concept_id,
            CAST(src.valuenum AS FLOAT)                         AS value_as_number,
            CAST(NULL AS INTEGER)                               AS value_as_concept_id,
            CAST(NULL AS INTEGER)                               AS unit_concept_id,
            CAST(NULL AS INTEGER)                               AS range_low,
            CAST(NULL AS INTEGER)                               AS range_high,
            CAST(NULL AS INTEGER)                               AS provider_id,
            src.episode_id                                      AS visit_occurrence_id,
            CAST(NULL AS INTEGER)                               AS visit_detail_id,
            src.itemid                                          AS measurement_source_value,
            CAST(NULL AS INTEGER)                               AS measurement_source_concept_id,
            src.valueuom                                        AS unit_source_value,
            src.value                                           AS value_source_value,
            'chartevents'                                       AS unit_id,
            src.load_table_id                                   AS load_table_id,
            src.load_row_id                                     AS load_row_id,
            src.trace_id                                        AS trace_id
        FROM  
            """ + etlSchemaName + """.src_chartevents src
        INNER JOIN """ + etlSchemaName + """.concept_mapping map
        ON map.concept_name = src.itemid
        WHERE src.value ~ '^[0-9\.]+$'
        ;
        """
    with con:
        with con.cursor() as cursor:
            log.info("Dropping table if exists: " + etlSchemaName + ".cdm_measurement")
            cursor.execute(dropQuery)
            log.info("Creating table: " + etlSchemaName + ".cdm_measurement")
            cursor.execute(createQuery)
            log.info("Loading table: " + etlSchemaName + ".cdm_measurement - Lab Events")
            cursor.execute(insertLabeventsQuery)
            log.info("Loading table: " + etlSchemaName + ".cdm_measurement - Chart Events")
            cursor.execute(insertCharteventsQuery)


def migrate(con, etlSchemaName):
    createMeasurements(con = con, etlSchemaName = etlSchemaName)
