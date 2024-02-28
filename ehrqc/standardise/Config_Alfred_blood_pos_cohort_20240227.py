import os


# database connection details
db_details = {
    "sql_host_name": os.environ['POSTGRES_HOSTNAME'],
    "sql_port_number": os.environ['POSTGRES_PORT_NUMBER'],
    "sql_user_name": os.environ['POSTGRES_USER_NAME'],
    "sql_password": os.environ['POSTGRES_PASSWORD'],
    "sql_db_name": 'omop_alfred',
}

source_schema_name = 'omop_migration_etl_20240122'

etl_schema_name = 'omop_migration_etl_20240122'

lookup_schema_name = 'omop_migration_etl_20240122'

vocabulary = {
    'concept': os.environ['EHR_DATA_BASE'] + '/data/vocabulary_download_v5/CONCEPT.csv',
    'vocabulary': os.environ['EHR_DATA_BASE'] + '/data/vocabulary_download_v5/VOCABULARY.csv',
    'domain': os.environ['EHR_DATA_BASE'] + '/data/vocabulary_download_v5/DOMAIN.csv',
    'concept_class': os.environ['EHR_DATA_BASE'] + '/data/vocabulary_download_v5/CONCEPT_CLASS.csv',
    'concept_relationship': os.environ['EHR_DATA_BASE'] + '/data/vocabulary_download_v5/CONCEPT_RELATIONSHIP.csv',
    'relationship': os.environ['EHR_DATA_BASE'] + '/data/vocabulary_download_v5/RELATIONSHIP.csv',
    'concept_synonym': os.environ['EHR_DATA_BASE'] + '/data/vocabulary_download_v5/CONCEPT_SYNONYM.csv',
    'concept_ancestor': os.environ['EHR_DATA_BASE'] + '/data/vocabulary_download_v5/CONCEPT_ANCESTOR.csv',
    'tmp_custom_mapping': os.environ['EHR_DATA_BASE'] + '/data/vocabulary_download_v5/tmp_custom_mapping.csv',
}

patients = {
    'file_name': os.environ['EHR_DATA_BASE'] + '/blood_pos_cohort_20240119/2024-01-17-blood-demography.csv',
    'file_separator': ',',
    'column_mapping': {
        'patient_id': 'PATIENT_ID',
        'age': None,
        'gender': 'SEX',
        'dob': 'DATE_OF_BIRTH_DATETIME',
        'dod': 'DATEOFDEATH_DATETIME',
    },
    'overwrite': True,
    'staging_sql': os.environ['EHR_QC_STANDARDISE_BASE'] + '/ehrqc/standardise/sql/blood_pos_cohort_20240119/stage_patients.sql'
}

admissions = {
    'file_name': os.environ['EHR_DATA_BASE'] + '/blood_pos_cohort_20240119/2023-11-10-blood-admissions.csv',
    'file_separator': ',',
    'column_mapping': {
        'patient_id': 'PATIENT_ID',
        'episode_id': 'EPISODE_ID',
        'admittime': 'Organism_FIRST_NOTED',
        'dischtime': 'end_date',
        'deathtime': 'DATEOFDEATH_DATETIME',
        'admission_type': 'TYPE',
        'admission_location': 'ADMITTING_WARD',
        'discharge_location': None,
        'insurance': None,
        'language': None,
        'marital_status': None,
        'ethnicity': None,
        'edregtime': None,
        'edouttime': None,
        'hospital_expire_flag': None,
    },
    'overwrite': True,
    'staging_sql': os.environ['EHR_QC_STANDARDISE_BASE'] + '/ehrqc/standardise/sql/blood_pos_cohort_20240119/stage_admissions.sql'
}

chartevents = {
    'file_name': os.environ['EHR_DATA_BASE'] + '/blood_pos_cohort_20240119/2023-11-10-vitalevents-unpivoted.csv',
    'file_separator': ',',
    'column_mapping': {
        'patient_id': 'PATIENT_ID',
        'episode_id': 'EPISODE_ID',
        'vital_id': 'VITAL_ID',
        'charttime': 'PERFORMED_DATETIME',
        'storetime': 'RESULT_UPDATE_DATETIME',
        'itemid': 'TYPE',
        'value': 'RESULT',
        'valuenum': 'RESULT',
        'valueuom': 'RESULT_UNITS',
        'warning': None,
    },
    'overwrite': True,
    'staging_sql': os.environ['EHR_QC_STANDARDISE_BASE'] + '/ehrqc/standardise/sql/blood_pos_cohort_20240119/stage_chartevents.sql'
}

labevents = {
    'file_name': os.environ['EHR_DATA_BASE'] + '/blood_pos_cohort_20240119/2023-11-10-labevents-unpivoted.aa.csv',
    'file_separator': ',',
    'column_mapping': {
        'labevent_id': 'PATH_RESULT_ID',
        'patient_id': 'PATIENT_ID',
        'episode_id': 'EPISODE_ID',
        'specimen_id': 'ORDER_ID',
        'itemid': 'TYPE',
        'charttime': 'PERFORMED_DATETIME',
        'storetime': 'RESULT_UPDT_DATETIME',
        'value': 'RESULT',
        'valuenum': 'RESULT',
        'valueuom': 'RESULT_UNITS',
        'ref_range_lower': 'NORMAL_LOW',
        'ref_range_upper': 'NORMAL_HIGH',
        'flag': None,
        'priority': None,
        'comments': None,
    },
    'overwrite': True,
    'staging_sql': os.environ['EHR_QC_STANDARDISE_BASE'] + '/ehrqc/standardise/sql/blood_pos_cohort_20240119/stage_labevents.sql'
}

# labevents = {
#     'file_name': os.environ['EHR_DATA_BASE'] + '/blood_pos_cohort_20240119/2023-11-10-labevents-unpivoted.ab.csv',
#     'file_separator': ',',
#     'column_mapping': {
#         'labevent_id': 'PATH_RESULT_ID',
#         'patient_id': 'PATIENT_ID',
#         'episode_id': 'EPISODE_ID',
#         'specimen_id': 'ORDER_ID',
#         'itemid': 'TYPE',
#         'charttime': 'PERFORMED_DATETIME',
#         'storetime': 'RESULT_UPDT_DATETIME',
#         'value': 'RESULT',
#         'valuenum': 'RESULT',
#         'valueuom': 'RESULT_UNITS',
#         'ref_range_lower': 'NORMAL_LOW',
#         'ref_range_upper': 'NORMAL_HIGH',
#         'flag': None,
#         'priority': None,
#         'comments': None,
#     },
#     'overwrite': False,
# }

# labevents = {
#     'file_name': os.environ['EHR_DATA_BASE'] + '/blood_pos_cohort_20240119/2023-11-10-labevents-unpivoted.ac.csv',
#     'file_separator': ',',
#     'column_mapping': {
#         'labevent_id': 'PATH_RESULT_ID',
#         'patient_id': 'PATIENT_ID',
#         'episode_id': 'EPISODE_ID',
#         'specimen_id': 'ORDER_ID',
#         'itemid': 'TYPE',
#         'charttime': 'PERFORMED_DATETIME',
#         'storetime': 'RESULT_UPDT_DATETIME',
#         'value': 'RESULT',
#         'valuenum': 'RESULT',
#         'valueuom': 'RESULT_UNITS',
#         'ref_range_lower': 'NORMAL_LOW',
#         'ref_range_upper': 'NORMAL_HIGH',
#         'flag': None,
#         'priority': None,
#         'comments': None,
#     },
#     'overwrite': False,
# }

# labevents = {
#     'file_name': os.environ['EHR_DATA_BASE'] + '/blood_pos_cohort_20240119/2023-11-10-labevents-unpivoted.ad.csv',
#     'file_separator': ',',
#     'column_mapping': {
#         'labevent_id': 'PATH_RESULT_ID',
#         'patient_id': 'PATIENT_ID',
#         'episode_id': 'EPISODE_ID',
#         'specimen_id': 'ORDER_ID',
#         'itemid': 'TYPE',
#         'charttime': 'PERFORMED_DATETIME',
#         'storetime': 'RESULT_UPDT_DATETIME',
#         'value': 'RESULT',
#         'valuenum': 'RESULT',
#         'valueuom': 'RESULT_UNITS',
#         'ref_range_lower': 'NORMAL_LOW',
#         'ref_range_upper': 'NORMAL_HIGH',
#         'flag': None,
#         'priority': None,
#         'comments': None,
#     },
#     'overwrite': False,
# }

# labevents = {
#     'file_name': os.environ['EHR_DATA_BASE'] + '/blood_pos_cohort_20240119/2023-11-10-labevents-unpivoted.ae.csv',
#     'file_separator': ',',
#     'column_mapping': {
#         'labevent_id': 'PATH_RESULT_ID',
#         'patient_id': 'PATIENT_ID',
#         'episode_id': 'EPISODE_ID',
#         'specimen_id': 'ORDER_ID',
#         'itemid': 'TYPE',
#         'charttime': 'PERFORMED_DATETIME',
#         'storetime': 'RESULT_UPDT_DATETIME',
#         'value': 'RESULT',
#         'valuenum': 'RESULT',
#         'valueuom': 'RESULT_UNITS',
#         'ref_range_lower': 'NORMAL_LOW',
#         'ref_range_upper': 'NORMAL_HIGH',
#         'flag': None,
#         'priority': None,
#         'comments': None,
#     },
#     'overwrite': False,
# }

# labevents = {
#     'file_name': os.environ['EHR_DATA_BASE'] + '/blood_pos_cohort_20240119/2023-11-10-labevents-unpivoted.af.csv',
#     'file_separator': ',',
#     'column_mapping': {
#         'labevent_id': 'PATH_RESULT_ID',
#         'patient_id': 'PATIENT_ID',
#         'episode_id': 'EPISODE_ID',
#         'specimen_id': 'ORDER_ID',
#         'itemid': 'TYPE',
#         'charttime': 'PERFORMED_DATETIME',
#         'storetime': 'RESULT_UPDT_DATETIME',
#         'value': 'RESULT',
#         'valuenum': 'RESULT',
#         'valueuom': 'RESULT_UNITS',
#         'ref_range_lower': 'NORMAL_LOW',
#         'ref_range_upper': 'NORMAL_HIGH',
#         'flag': None,
#         'priority': None,
#         'comments': None,
#     },
#     'overwrite': False,
# }

# labevents = {
#     'file_name': os.environ['EHR_DATA_BASE'] + '/blood_pos_cohort_20240119/2023-11-10-labevents-unpivoted.ag.csv',
#     'file_separator': ',',
#     'column_mapping': {
#         'labevent_id': 'PATH_RESULT_ID',
#         'patient_id': 'PATIENT_ID',
#         'episode_id': 'EPISODE_ID',
#         'specimen_id': 'ORDER_ID',
#         'itemid': 'TYPE',
#         'charttime': 'PERFORMED_DATETIME',
#         'storetime': 'RESULT_UPDT_DATETIME',
#         'value': 'RESULT',
#         'valuenum': 'RESULT',
#         'valueuom': 'RESULT_UNITS',
#         'ref_range_lower': 'NORMAL_LOW',
#         'ref_range_upper': 'NORMAL_HIGH',
#         'flag': None,
#         'priority': None,
#         'comments': None,
#     },
#     'overwrite': False,
# }

# labevents = {
#     'file_name': os.environ['EHR_DATA_BASE'] + '/blood_pos_cohort_20240119/2023-11-10-labevents-unpivoted.ah.csv',
#     'file_separator': ',',
#     'column_mapping': {
#         'labevent_id': 'PATH_RESULT_ID',
#         'patient_id': 'PATIENT_ID',
#         'episode_id': 'EPISODE_ID',
#         'specimen_id': 'ORDER_ID',
#         'itemid': 'TYPE',
#         'charttime': 'PERFORMED_DATETIME',
#         'storetime': 'RESULT_UPDT_DATETIME',
#         'value': 'RESULT',
#         'valuenum': 'RESULT',
#         'valueuom': 'RESULT_UNITS',
#         'ref_range_lower': 'NORMAL_LOW',
#         'ref_range_upper': 'NORMAL_HIGH',
#         'flag': None,
#         'priority': None,
#         'comments': None,
#     },
#     'overwrite': False,
# }

# labevents = {
#     'file_name': os.environ['EHR_DATA_BASE'] + '/blood_pos_cohort_20240119/2023-11-10-labevents-unpivoted.ai.csv',
#     'file_separator': ',',
#     'column_mapping': {
#         'labevent_id': 'PATH_RESULT_ID',
#         'patient_id': 'PATIENT_ID',
#         'episode_id': 'EPISODE_ID',
#         'specimen_id': 'ORDER_ID',
#         'itemid': 'TYPE',
#         'charttime': 'PERFORMED_DATETIME',
#         'storetime': 'RESULT_UPDT_DATETIME',
#         'value': 'RESULT',
#         'valuenum': 'RESULT',
#         'valueuom': 'RESULT_UNITS',
#         'ref_range_lower': 'NORMAL_LOW',
#         'ref_range_upper': 'NORMAL_HIGH',
#         'flag': None,
#         'priority': None,
#         'comments': None,
#     },
#     'overwrite': False,
# }

# labevents = {
#     'file_name': os.environ['EHR_DATA_BASE'] + '/blood_pos_cohort_20240119/2023-11-10-labevents-unpivoted.aj.csv',
#     'file_separator': ',',
#     'column_mapping': {
#         'labevent_id': 'PATH_RESULT_ID',
#         'patient_id': 'PATIENT_ID',
#         'episode_id': 'EPISODE_ID',
#         'specimen_id': 'ORDER_ID',
#         'itemid': 'TYPE',
#         'charttime': 'PERFORMED_DATETIME',
#         'storetime': 'RESULT_UPDT_DATETIME',
#         'value': 'RESULT',
#         'valuenum': 'RESULT',
#         'valueuom': 'RESULT_UNITS',
#         'ref_range_lower': 'NORMAL_LOW',
#         'ref_range_upper': 'NORMAL_HIGH',
#         'flag': None,
#         'priority': None,
#         'comments': None,
#     },
#     'overwrite': False,
# }

conceptmaps = {
    'file_name': os.environ['EHR_DATA_BASE'] + '/blood_pos_cohort_20240119/concepts_mapped.csv',
    'file_separator': '\t',
    'column_mapping': {
        'concept_name': 'searchPhrase',
        'snomed_concept_id': 'snomedConceptId',
    },
}
