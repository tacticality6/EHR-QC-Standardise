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
    __schema_name__.admissions
;
