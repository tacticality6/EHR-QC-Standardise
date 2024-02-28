SELECT
    ce.patient_id  AS patient_id,
    ce.episode_id  AS episode_id,
    ce.vital_id     AS vital_id,
    ce.charttime   AS charttime,
    ce.storetime   AS storetime,
    ce.itemid      AS itemid,
    ce.value       AS value,
    ce.valuenum    AS valuenum,
    ce.valueuom    AS valueuom,
    ce.warning    AS warning,
    'chartevents'                       AS load_table_id,
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
    jsonb_build_object('patient_id', ce.patient_id, 'episode_id', ce.episode_id, 'vital_id', ce.vital_id, 'charttime', ce.charttime)                                 AS trace_id
FROM
    __schema_name__.chartevents ce
INNER JOIN __schema_name__.src_admissions adm
ON adm.episode_id = ce.episode_id
;
