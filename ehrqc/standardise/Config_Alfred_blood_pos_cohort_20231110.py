import os


# database connection details
db_details = {
    "sql_host_name": 'localhost',
    "sql_port_number": 5434,
    "sql_user_name": 'postgres',
    "sql_password": 'mysecretpassword',
    "sql_db_name": 'omop_alfred',
}

source_schema_name = 'omop_migration_etl_20231110'

etl_schema_name = 'omop_migration_etl_20231110'

lookup_schema_name = 'omop_migration_etl_20231110'

vocabulary = {
    'concept': os.environ['EICU_EHR_PIPELINE_BASE'] + '/data/vocabulary_download_v5/CONCEPT.csv',
    'vocabulary': os.environ['EICU_EHR_PIPELINE_BASE'] + '/data/vocabulary_download_v5/VOCABULARY.csv',
    'domain': os.environ['EICU_EHR_PIPELINE_BASE'] + '/data/vocabulary_download_v5/DOMAIN.csv',
    'concept_class': os.environ['EICU_EHR_PIPELINE_BASE'] + '/data/vocabulary_download_v5/CONCEPT_CLASS.csv',
    'concept_relationship': os.environ['EICU_EHR_PIPELINE_BASE'] + '/data/vocabulary_download_v5/CONCEPT_RELATIONSHIP.csv',
    'relationship': os.environ['EICU_EHR_PIPELINE_BASE'] + '/data/vocabulary_download_v5/RELATIONSHIP.csv',
    'concept_synonym': os.environ['EICU_EHR_PIPELINE_BASE'] + '/data/vocabulary_download_v5/CONCEPT_SYNONYM.csv',
    'concept_ancestor': os.environ['EICU_EHR_PIPELINE_BASE'] + '/data/vocabulary_download_v5/CONCEPT_ANCESTOR.csv',
    'tmp_custom_mapping': os.environ['EICU_EHR_PIPELINE_BASE'] + '/data/vocabulary_download_v5/tmp_custom_mapping.csv',
}

patients = {
    'file_name': os.environ['EHR_DATA_BASE'] + '/blood_pos_cohort_20231110/2023-11-10-blood-demography.csv',
    'file_separator': ',',
    'column_mapping': {
        'patient_id': 'PATIENT_ID',
        'age': None,
        'gender': 'SEX',
        'dob': 'DATE_OF_BIRTH_DATETIME',
        'dod': 'DATEOFDEATH_DATETIME',
    },
}

admissions = {
    'file_name': os.environ['EHR_DATA_BASE'] + '/blood_pos_cohort_20231110/2023-11-10-blood-admissions.csv',
    'file_separator': ',',
    'column_mapping': {
        'patient_id': 'PATIENT_ID',
        'episode_id': 'EPISODE_ID',
        'admittime': 'start_date',
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
}

chartevents = {
    'file_name': os.environ['EHR_DATA_BASE'] + '/blood_pos_cohort_20231110/2023-11-10-vitalevents-unpivoted.csv',
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
}

labevents = {
    'file_name': os.environ['EHR_DATA_BASE'] + '/blood_pos_cohort_20231110/2023-11-10-labevents-unpivoted.csv',
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
}

conceptmaps = {
    'file_name': os.environ['EHR_DATA_BASE'] + '/blood_pos_cohort_20231110/concepts_mapped.csv',
    'file_separator': '\t',
    'column_mapping': {
        'concept_name': 'searchPhrase',
        'snomed_concept_id': 'snomedConceptId',
    },
}

