SELECT
    DISTINCT ON (pat.patient_id)
    pat.patient_id                          AS patient_id,
    pat.gender                              AS gender,
    pat.age                                 AS age,
    pat.dod                                 AS dod,
    pat.dob                                 AS dob,
    'patients'                          AS load_table_id,
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
    jsonb_build_object('patient_id', pat.patient_id)          AS trace_id
FROM
__schema_name__.patients pat
INNER JOIN __schema_name__.admissions adm
ON adm.patient_id = pat.patient_id
INNER JOIN __schema_name__.cohort coh
ON coh.episode_id = adm.episode_id
;
