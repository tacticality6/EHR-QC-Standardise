import logging

log = logging.getLogger("EHR-QC")


def createPatientsStaging(con, sourceSchemaName, destinationSchemaName):
    log.info("Creating staging table: " + destinationSchemaName + ".src_patients")
    dropQuery = """drop table if exists """ + destinationSchemaName + """.src_patients cascade"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.src_patients AS
        SELECT
            patient_id                          AS patient_id,
            gender                              AS gender,
            age                                 AS age,
            dod                                 AS dod,
            dob                                 AS dob,
            'patients'                          AS load_table_id,
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
            jsonb_build_object('patient_id', patient_id)                                  AS trace_id
        FROM
            """ + sourceSchemaName + """.patients
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createAdmissionsStaging(con, sourceSchemaName, destinationSchemaName):
    log.info("Creating staging table: " + destinationSchemaName + ".src_admissions")
    dropQuery = """drop table if exists """ + destinationSchemaName + """.src_admissions cascade"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.src_admissions AS
        SELECT
            patient_id                          AS patient_id,
            episode_id                          AS episode_id,
            admittime                           AS admittime,
            dischtime                           AS dischtime,
            deathtime                           AS deathtime,
            admission_type                      AS admission_type,
            admission_location                  AS admission_location,
            discharge_location                  AS discharge_location,
            insurance                           AS insurance,
            language                            AS language,
            marital_status                      AS marital_status,
            ethnicity                           AS ethnicity,
            edregtime                           AS edregtime,
            edouttime                           AS edouttime,
            hospital_expire_flag                AS hospital_expire_flag,
            'admissions'                        AS load_table_id,
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
            jsonb_build_object('patient_id', patient_id, 'episode_id', episode_id)                                  AS trace_id
        FROM
            """ + sourceSchemaName + """.admissions
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createChartEventsStaging(con, sourceSchemaName, destinationSchemaName):
    log.info("Creating staging table: " + destinationSchemaName + ".src_chartevents")
    dropQuery = """drop table if exists """ + destinationSchemaName + """.src_chartevents cascade"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.src_chartevents AS
        SELECT
            patient_id  AS patient_id,
            episode_id  AS episode_id,
            vital_id     AS vital_id,
            charttime   AS charttime,
            storetime   AS storetime,
            itemid      AS itemid,
            value       AS value,
            valuenum    AS valuenum,
            valueuom    AS valueuom,
            warning    AS warning,
            'chartevents'                       AS load_table_id,
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
            jsonb_build_object('patient_id', patient_id, 'episode_id', episode_id, 'vital_id', vital_id, 'charttime', charttime)                                 AS trace_id
        FROM
            """ + sourceSchemaName + """.chartevents
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createLabEventsStaging(con, sourceSchemaName, destinationSchemaName):
    log.info("Creating staging table: " + destinationSchemaName + ".src_labevents")
    dropQuery = """drop table if exists """ + destinationSchemaName + """.src_labevents cascade"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.src_labevents AS
        SELECT
            labevent_id                         AS labevent_id,
            patient_id                          AS patient_id,
            episode_id                          AS episode_id,
            specimen_id                          AS specimen_id,
            itemid                              AS itemid,
            charttime                           AS charttime,
            storetime                           AS storetime,
            value                               AS value,
            valuenum                            AS valuenum,
            valueuom                            AS valueuom,
            ref_range_lower                     AS ref_range_lower,
            ref_range_upper                     AS ref_range_upper,
            flag                                AS flag,
            priority                            AS priority,
            comments                            AS comments,
            'labevents'                         AS load_table_id,
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
            jsonb_build_object('patient_id', patient_id, 'episode_id', episode_id, 'labevent_id', labevent_id, 'charttime', charttime)                                 AS trace_id
        FROM
            """ + sourceSchemaName + """.labevents
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def migrate(con, sourceSchemaName, destinationSchemaName):
    createPatientsStaging(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    createAdmissionsStaging(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    createChartEventsStaging(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    createLabEventsStaging(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
