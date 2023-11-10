WITH stg1 AS (
    SELECT
        DISTINCT ON (adm.patient_id, adm.episode_id)
        adm.patient_id                          AS patient_id,
        adm.episode_id                          AS episode_id,
        (adm.edouttime || '-' || floor(random() * 11 + 1)::int::text || '-' || floor(random() * 28 + 1)::int::text || ' ' || adm.admittime)::timestamp AS admittime,
        adm.dischtime                           AS dischtime,
        adm.deathtime                           AS deathtime,
        adm.admission_type                      AS admission_type,
        adm.admission_location                  AS admission_location,
        adm.discharge_location                  AS discharge_location,
        adm.insurance                           AS insurance,
        adm.language                            AS language,
        adm.marital_status                      AS marital_status,
        adm.ethnicity                           AS ethnicity,
        adm.edregtime                           AS edregtime,
        adm.edouttime                           AS edouttime,
        adm.hospital_expire_flag                AS hospital_expire_flag,
        'admissions'                            AS load_table_id,
        ('x'||substr(md5(random():: text),1,8))::bit(32)::int AS load_row_id,
        jsonb_build_object('patient_id', adm.patient_id, 'episode_id', adm.episode_id) AS trace_id
    FROM
        __schema_name__.admissions adm
    INNER JOIN __schema_name__.cohort coh
    ON coh.episode_id = adm.episode_id
    ORDER BY adm.patient_id, adm.episode_id, adm.admittime ASC
)
SELECT
stg1.patient_id AS patient_id,
stg1.episode_id AS episode_id,
stg1.admittime AS admittime,
stg1.admittime + (stg1.dischtime || ' minutes')::interval AS dischtime,
stg1.deathtime AS deathtime,
stg1.admission_type AS admission_type,
stg1.admission_location AS admission_location,
stg1.discharge_location AS discharge_location,
stg1.insurance AS insurance,
stg1.language AS language,
stg1.marital_status AS marital_status,
stg1.ethnicity AS ethnicity,
stg1.edregtime AS edregtime,
stg1.edouttime AS edouttime,
stg1.hospital_expire_flag AS hospital_expire_flag,
stg1.load_table_id AS load_table_id,
stg1.load_row_id AS load_row_id,
stg1.trace_id AS trace_id
FROM
stg1
;
