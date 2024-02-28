SELECT
    le.labevent_id                         AS labevent_id,
    le.patient_id                          AS patient_id,
    le.episode_id                          AS episode_id,
    le.specimen_id                          AS specimen_id,
    le.itemid                              AS itemid,
    le.charttime                           AS charttime,
    le.storetime                           AS storetime,
    le.value                               AS value,
    le.valuenum                            AS valuenum,
    le.valueuom                            AS valueuom,
    le.ref_range_lower                     AS ref_range_lower,
    le.ref_range_upper                     AS ref_range_upper,
    le.flag                                AS flag,
    le.priority                            AS priority,
    le.comments                            AS comments,
    'labevents'                         AS load_table_id,
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
    jsonb_build_object('patient_id', le.patient_id, 'episode_id', le.episode_id, 'labevent_id', le.labevent_id, 'charttime', le.charttime)                                 AS trace_id
FROM
    __schema_name__.labevents le
INNER JOIN __schema_name__.src_admissions adm
ON adm.episode_id = le.episode_id
;
