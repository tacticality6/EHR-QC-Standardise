import os

# database connection details
db_details = {
    "sql_host_name": os.environ['POSTGRES_HOSTNAME'],
    "sql_port_number": os.environ['POSTGRES_PORT_NUMBER'],
    "sql_user_name": os.environ['POSTGRES_USER_NAME'],
    "sql_password": os.environ['POSTGRES_PASSWORD'],
    "sql_db_name": os.environ['POSTGRES_DB_NAME'],
}

source_schema_name = 'eicu_etl_20231106'

etl_schema_name = 'eicu_etl_20231106'

lookup_schema_name = 'eicu_etl_20231106'

vocabulary = {
    'concept': '/superbugai-data/vocabulary_download_v5/CONCEPT.csv',
    'vocabulary': '/superbugai-data/vocabulary_download_v5/VOCABULARY.csv',
    'domain': '/superbugai-data/vocabulary_download_v5/DOMAIN.csv',
    'concept_class': '/superbugai-data/vocabulary_download_v5/CONCEPT_CLASS.csv',
    'concept_relationship': '/superbugai-data/vocabulary_download_v5/CONCEPT_RELATIONSHIP.csv',
    'relationship': '/superbugai-data/vocabulary_download_v5/RELATIONSHIP.csv',
    'concept_synonym': '/superbugai-data/vocabulary_download_v5/CONCEPT_SYNONYM.csv',
    'concept_ancestor': '/superbugai-data/vocabulary_download_v5/CONCEPT_ANCESTOR.csv',
    'tmp_custom_mapping': '/superbugai-data/vocabulary_download_v5/tmp_custom_mapping.csv',
}

patients = {
    'file_name': os.environ['EICU_EHR_PIPELINE_BASE'] + '/data/eICU/patient.csv',
    'file_separator': ',',
    'column_mapping': {
        'patient_id': 'uniquepid',
        'age': 'age',
        'gender': 'gender',
        'dob': None,
        'dod': None,
    },
    'overwrite': True,
    'staging_sql': os.environ['EHR_QC_STANDARDISE_BASE'] + '/ehrqc/standardise/sql/stage_patients.sql'
}

admissions = {
    'file_name': os.environ['EICU_EHR_PIPELINE_BASE'] + '/data/eICU/patient.csv',
    'file_separator': ',',
    'column_mapping': {
        'patient_id': 'uniquepid',
        'episode_id': 'patientunitstayid',
        'admittime': 'unitadmittime24',
        'dischtime': 'unitdischargeoffset',
        'deathtime': None,
        'admission_type': 'unittype',
        'admission_location': 'hospitaladmitsource',
        'discharge_location': 'hospitaldischargelocation',
        'insurance': None,
        'language': None,
        'marital_status': None,
        'ethnicity': None,
        'edregtime': None,
        'edouttime': 'hospitaldischargeyear',
        'hospital_expire_flag': 'unitdischargestatus',
    },
    'overwrite': True,
    'staging_sql': os.environ['EHR_QC_STANDARDISE_BASE'] + '/ehrqc/standardise/sql/stage_admissions.sql'
}

# chartevents = {
#     'file_name': os.environ['EICU_EHR_PIPELINE_BASE'] + '/home/yram0006/phd/chapter_2/workspace/EHR-QC-Demo/2023_11_aicare/data/lab.csv',
#     'file_separator': ',',
#     'column_mapping': {
#         'patient_id': None,
#         'episode_id': 'patientunitstayid',
#         'vital_id': 'vitalaperiodicid',
#         'charttime': 'observationoffset',
#         'storetime': None,
#         'itemid': 'TYPE',
#         'value': 'RESULT',
#         'valuenum': 'RESULT',
#         'valueuom': 'RESULT_UNITS',
#         'warning': None,
#     },
#     'overwrite': True,
# }

# diagnosis = {
#     'file_name': os.environ['EICU_EHR_PIPELINE_BASE'] + '/data/eICU/diagnosis.csv',
#     'file_separator': ',',
#     'column_mapping': {
#         'diagnosis_id': 'diagnosisid',
#         'episode_id': 'patientunitstayid',
#         'patient_id': None,
#         'charttime': 'diagnosisoffset',
#         'diagnosis': 'icd9code',
#         'diagnosis_description': 'diagnosisstring',
#     },
#     'overwrite': True,
# }

labevents = {
    'file_name': os.environ['EICU_EHR_PIPELINE_BASE'] + '/data/eICU/lab.csv',
    'file_separator': ',',
    'column_mapping': {
        'labevent_id': 'labid',
        'patient_id': None,
        'episode_id': 'patientunitstayid',
        'specimen_id': None,
        'itemid': 'labname',
        'charttime': 'labresultoffset',
        'storetime': None,
        'value': 'labresulttext',
        'valuenum': 'labresult',
        'valueuom': 'labmeasurenamesystem',
        'ref_range_lower': None,
        'ref_range_upper': None,
        'flag': None,
        'priority': None,
        'comments': None,
    },
    'overwrite': True,
    'staging_sql': os.environ['EHR_QC_STANDARDISE_BASE'] + '/ehrqc/standardise/sql/stage_labevents.sql',
}

conceptmaps = {
    'file_name': os.environ['EICU_EHR_PIPELINE_BASE'] + '/data/concept_mapping/concept_labs_mapped.csv',
    'file_separator': '\t',
    'column_mapping': {
        'concept_name': 'searchPhrase',
        'concept_id': 'reviewedConceptId',
    },
}

