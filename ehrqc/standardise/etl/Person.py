import logging

log = logging.getLogger("EHR-QC")


def createPersonCdm(con, etlSchemaName):
    dropQuery = """drop table if exists """ + etlSchemaName + """.cdm_person cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.cdm_person
        (
            person_id                   INTEGER   not null  ,
            gender_concept_id           INTEGER   not null  ,
            year_of_birth               INTEGER             ,
            month_of_birth              INTEGER             ,
            day_of_birth                INTEGER             ,
            birth_datetime              TIMESTAMP           ,
            race_concept_id             INTEGER             ,
            ethnicity_concept_id        INTEGER             ,
            location_id                 INTEGER             ,
            provider_id                 INTEGER             ,
            care_site_id                INTEGER             ,
            person_source_value         TEXT                ,
            gender_source_value         TEXT                ,
            gender_source_concept_id    INTEGER             ,
            race_source_value           TEXT                ,
            race_source_concept_id      INTEGER             ,
            ethnicity_source_value      TEXT                ,
            ethnicity_source_concept_id INTEGER             ,
            unit_id                     TEXT                ,
            load_table_id               TEXT                ,
            load_row_id                 INTEGER             ,
            trace_id                    TEXT
        )
        ;
        """
    insertQuery = """INSERT INTO """ + etlSchemaName + """.cdm_person
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int       AS person_id,
            CASE
                    WHEN p.gender = 'Female' THEN 8532 -- FEMALE
                    WHEN p.gender = 'Male' THEN 8507 -- MALE
                    WHEN p.gender = 'Other' THEN 8521 -- MALE
                    WHEN p.gender = 'Not Specified' THEN 8551 -- MALE
                    WHEN p.gender = 'Indeterminate Baby' THEN 8570 -- MALE
                    WHEN p.gender = 'Not stated/inadequately described' THEN 8570 -- MALE
                    ELSE 0
            END                                                         AS gender_concept_id,
            DATE_PART('year', p.dob)                                    AS year_of_birth,
            DATE_PART('month', p.dob)                                   AS month_of_birth,
            DATE_PART('day', p.dob)                                     AS day_of_birth,
            p.dob                                                       AS birth_DATETIME,
            0                                                           AS race_concept_id,
            0                                                           AS ethnicity_concept_id,
            CAST(NULL AS INTEGER)                                       AS location_id,
            CAST(NULL AS INTEGER)                                       AS provider_id,
            CAST(NULL AS INTEGER)                                       AS care_site_id,
            CAST(p.patient_id AS TEXT)                                  AS person_source_value,
            p.gender                                                    AS gender_source_value,
            0                                                           AS gender_source_concept_id,
            0                                                           AS race_source_value,
            0                                                           AS race_source_concept_id,
            0                                                           AS ethnicity_source_value,
            0                                                           AS ethnicity_source_concept_id,
            'person.patients'                                           AS unit_id,
            p.load_table_id                                             AS load_table_id,
            p.load_row_id                                               AS load_row_id,
            p.trace_id                                                  AS trace_id
        FROM
            """ + etlSchemaName + """.src_patients p
        ;
        """
    with con:
        with con.cursor() as cursor:
            log.info("Dropping table if exists: " + etlSchemaName + ".cdm_person")
            cursor.execute(dropQuery)
            log.info("Creating table: " + etlSchemaName + ".cdm_person")
            cursor.execute(createQuery)
            log.info("Loading table: " + etlSchemaName + ".cdm_person")
            cursor.execute(insertQuery)


def migrate(con, etlSchemaName):
    createPersonCdm(con = con, etlSchemaName = etlSchemaName)
