from ehrqc.standardize import Config
from ehrqc.standardize import Utils

import logging

log = logging.getLogger("EHR-QC")


def importPatients(con, sourceSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + sourceSchemaName + ".patients")

    dropQuery = """DROP TABLE IF EXISTS """ + sourceSchemaName + """.patients CASCADE"""
    createQuery = """CREATE TABLE """ + sourceSchemaName + """.patients
        (
            patient_id INT NOT NULL,
            gender VARCHAR(50),
            age INT,
            dod TIMESTAMP(0),
            dob TIMESTAMP(0),

            CONSTRAINT pat_patid_unique UNIQUE (patient_id),
            CONSTRAINT pat_patid_pk PRIMARY KEY (patient_id)
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd
    import numpy as np

    df = pd.read_csv(filePath, sep=fileSeparator)
    df[Config.patients['column_mapping']['dod']].replace({np.nan: None}, inplace=True)
    df[Config.patients['column_mapping']['dob']].replace({np.nan: None}, inplace=True)
    dfColumns = []
    columns = []
    if(Config.patients['column_mapping']['patient_id']):
        dfColumns.append(Config.patients['column_mapping']['patient_id'])
        columns.append('patient_id')
    if(Config.patients['column_mapping']['gender']):
        dfColumns.append(Config.patients['column_mapping']['gender'])
        columns.append('gender')
    if(Config.patients['column_mapping']['age']):
        dfColumns.append(Config.patients['column_mapping']['age'])
        columns.append('age')
    if(Config.patients['column_mapping']['dod']):
        dfColumns.append(Config.patients['column_mapping']['dod'])
        columns.append('dod')
    if(Config.patients['column_mapping']['dob']):
        dfColumns.append(Config.patients['column_mapping']['dob'])
        columns.append('dob')

    Utils.saveDataframe(con=con, destinationSchemaName=sourceSchemaName, destinationTableName='patients', columns=columns, df=df, dfColumns=dfColumns)


def importAdmissions(con, sourceSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + sourceSchemaName + ".admissions")

    dropQuery = """DROP TABLE IF EXISTS """ + sourceSchemaName + """.admissions CASCADE"""
    createQuery = """CREATE TABLE """ + sourceSchemaName + """.admissions
        (
            patient_id INT NOT NULL,
            episode_id INT NOT NULL,
            admittime TIMESTAMP(0),
            dischtime TIMESTAMP(0),
            deathtime TIMESTAMP(0),
            admission_type VARCHAR(50),
            admission_location VARCHAR(50),
            discharge_location VARCHAR(50),
            insurance VARCHAR(255),
            language VARCHAR(10),
            marital_status VARCHAR(50),
            ethnicity VARCHAR(200),
            edregtime TIMESTAMP(0),
            edouttime TIMESTAMP(0),
            hospital_expire_flag SMALLINT
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd
    import numpy as np

    df = pd.read_csv(filePath, sep=fileSeparator)
    df[Config.admissions['column_mapping']['admittime']].replace({np.nan: None}, inplace=True)
    df[Config.admissions['column_mapping']['dischtime']].replace({np.nan: None}, inplace=True)
    df[Config.admissions['column_mapping']['deathtime']].replace({np.nan: None}, inplace=True)
    dfColumns = []
    columns = []
    if(Config.admissions['column_mapping']['patient_id']):
        dfColumns.append(Config.admissions['column_mapping']['patient_id'])
        columns.append('patient_id')
    if(Config.admissions['column_mapping']['episode_id']):
        dfColumns.append(Config.admissions['column_mapping']['episode_id'])
        columns.append('episode_id')
    if(Config.admissions['column_mapping']['admittime']):
        dfColumns.append(Config.admissions['column_mapping']['admittime'])
        columns.append('admittime')
    if(Config.admissions['column_mapping']['dischtime']):
        dfColumns.append(Config.admissions['column_mapping']['dischtime'])
        columns.append('dischtime')
    if(Config.admissions['column_mapping']['deathtime']):
        dfColumns.append(Config.admissions['column_mapping']['deathtime'])
        columns.append('deathtime')
    if(Config.admissions['column_mapping']['admission_type']):
        dfColumns.append(Config.admissions['column_mapping']['admission_type'])
        columns.append('admission_type')
    if(Config.admissions['column_mapping']['admission_location']):
        dfColumns.append(Config.admissions['column_mapping']['admission_location'])
        columns.append('admission_location')
    if(Config.admissions['column_mapping']['discharge_location']):
        dfColumns.append(Config.admissions['column_mapping']['discharge_location'])
        columns.append('discharge_location')
    if(Config.admissions['column_mapping']['insurance']):
        dfColumns.append(Config.admissions['column_mapping']['insurance'])
        columns.append('insurance')
    if(Config.admissions['column_mapping']['language']):
        dfColumns.append(Config.admissions['column_mapping']['language'])
        columns.append('language')
    if(Config.admissions['column_mapping']['marital_status']):
        dfColumns.append(Config.admissions['column_mapping']['marital_status'])
        columns.append('marital_status')
    if(Config.admissions['column_mapping']['ethnicity']):
        dfColumns.append(Config.admissions['column_mapping']['ethnicity'])
        columns.append('ethnicity')
    if(Config.admissions['column_mapping']['edregtime']):
        dfColumns.append(Config.admissions['column_mapping']['edregtime'])
        columns.append('edregtime')
    if(Config.admissions['column_mapping']['edouttime']):
        dfColumns.append(Config.admissions['column_mapping']['edouttime'])
        columns.append('edouttime')
    if(Config.admissions['column_mapping']['hospital_expire_flag']):
        dfColumns.append(Config.admissions['column_mapping']['hospital_expire_flag'])
        columns.append('hospital_expire_flag')
    Utils.saveDataframe(con=con, destinationSchemaName=sourceSchemaName, destinationTableName='admissions', columns=columns, df=df, dfColumns=dfColumns)


def importChartEvents(con, sourceSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + sourceSchemaName + ".chartevents")

    dropQuery = """DROP TABLE IF EXISTS """ + sourceSchemaName + """.chartevents CASCADE"""
    createQuery = """CREATE TABLE """ + sourceSchemaName + """.chartevents
        (
            patient_id INT NOT NULL,
            episode_id INT NOT NULL,
            vital_id INT,
            charttime TIMESTAMP(0),
            storetime TIMESTAMP(0),
            itemid VARCHAR(160),
            value VARCHAR(160),
            valuenum VARCHAR(160),
            valueuom VARCHAR(20),
            warning SMALLINT
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd
    import numpy as np

    log.info("Reading file: " + str(filePath))
    df = pd.read_csv(filePath, sep=fileSeparator)
    dfColumns = []
    columns = []
    if(Config.chartevents['column_mapping']['patient_id']):
        dfColumns.append(Config.chartevents['column_mapping']['patient_id'])
        columns.append('patient_id')
    if(Config.chartevents['column_mapping']['episode_id']):
        dfColumns.append(Config.chartevents['column_mapping']['episode_id'])
        columns.append('episode_id')
    if(Config.chartevents['column_mapping']['vital_id']):
        dfColumns.append(Config.chartevents['column_mapping']['vital_id'])
        columns.append('vital_id')
    if(Config.chartevents['column_mapping']['charttime']):
        df[Config.chartevents['column_mapping']['charttime']].replace({np.nan: None}, inplace=True)
        dfColumns.append(Config.chartevents['column_mapping']['charttime'])
        columns.append('charttime')
    if(Config.chartevents['column_mapping']['storetime']):
        df[Config.chartevents['column_mapping']['storetime']].replace({np.nan: None}, inplace=True)
        dfColumns.append(Config.chartevents['column_mapping']['storetime'])
        columns.append('storetime')
    if(Config.chartevents['column_mapping']['itemid']):
        dfColumns.append(Config.chartevents['column_mapping']['itemid'])
        columns.append('itemid')
    if(Config.chartevents['column_mapping']['value']):
        # df = df[df[Config.chartevents['column_mapping']['value']].str.strip() != '']
        dfColumns.append(Config.chartevents['column_mapping']['value'])
        columns.append('value')
    if(Config.chartevents['column_mapping']['valuenum']):
        dfColumns.append(Config.chartevents['column_mapping']['valuenum'])
        columns.append('valuenum')
    if(Config.chartevents['column_mapping']['valueuom']):
        dfColumns.append(Config.chartevents['column_mapping']['valueuom'])
        columns.append('valueuom')
    if(Config.chartevents['column_mapping']['warning']):
        dfColumns.append(Config.chartevents['column_mapping']['warning'])
        columns.append('warning')
    Utils.saveDataframe(con=con, destinationSchemaName=sourceSchemaName, destinationTableName='chartevents', columns=columns, df=df, dfColumns=dfColumns)


def importLabEvents(con, sourceSchemaName, filePath, fileSeparator, createSchema=True):

    log.info("Creating table: " + sourceSchemaName + ".labevents")

    dropQuery = """DROP TABLE IF EXISTS """ + sourceSchemaName + """.labevents CASCADE"""
    createQuery = """CREATE TABLE """ + sourceSchemaName + """.labevents
        (
            labevent_id INT,
            patient_id INT NOT NULL,
            episode_id INT NOT NULL,
            specimen_id VARCHAR(20),
            itemid VARCHAR(200),
            charttime TIMESTAMP,
            storetime TIMESTAMP,
            value VARCHAR(200),
            valuenum VARCHAR(200),
            valueuom VARCHAR(20),
            ref_range_lower VARCHAR(200),
            ref_range_upper  VARCHAR(200),
            flag VARCHAR(10),
            priority VARCHAR(7),
            comments VARCHAR(620)
        )
        ;
        """
    if createSchema:
        with con:
            with con.cursor() as cursor:
                cursor.execute(dropQuery)
                cursor.execute(createQuery)

    import pandas as pd
    import numpy as np

    df = pd.read_csv(filePath, sep=fileSeparator)
    dfColumns = []
    columns = []
    if(Config.labevents['column_mapping']['labevent_id']):
        dfColumns.append(Config.labevents['column_mapping']['labevent_id'])
        columns.append('labevent_id')
    if(Config.labevents['column_mapping']['patient_id']):
        dfColumns.append(Config.labevents['column_mapping']['patient_id'])
        columns.append('patient_id')
    if(Config.labevents['column_mapping']['episode_id']):
        dfColumns.append(Config.labevents['column_mapping']['episode_id'])
        columns.append('episode_id')
    if(Config.labevents['column_mapping']['specimen_id']):
        dfColumns.append(Config.labevents['column_mapping']['specimen_id'])
        columns.append('specimen_id')
    if(Config.labevents['column_mapping']['itemid']):
        dfColumns.append(Config.labevents['column_mapping']['itemid'])
        columns.append('itemid')
    if(Config.labevents['column_mapping']['charttime']):
        df[Config.labevents['column_mapping']['charttime']].replace({np.nan: None}, inplace=True)
        dfColumns.append(Config.labevents['column_mapping']['charttime'])
        columns.append('charttime')
    if(Config.labevents['column_mapping']['storetime']):
        df[Config.labevents['column_mapping']['storetime']].replace({np.nan: None}, inplace=True)
        dfColumns.append(Config.labevents['column_mapping']['storetime'])
        columns.append('storetime')
    if(Config.labevents['column_mapping']['value']):
        # df = df[df[Config.labevents['column_mapping']['value']].str.strip() != '']
        dfColumns.append(Config.labevents['column_mapping']['value'])
        columns.append('value')
    if(Config.labevents['column_mapping']['valuenum']):
        dfColumns.append(Config.labevents['column_mapping']['valuenum'])
        columns.append('valuenum')
    if(Config.labevents['column_mapping']['valueuom']):
        dfColumns.append(Config.labevents['column_mapping']['valueuom'])
        columns.append('valueuom')
    if(Config.labevents['column_mapping']['ref_range_lower']):
        dfColumns.append(Config.labevents['column_mapping']['ref_range_lower'])
        columns.append('ref_range_lower')
    if(Config.labevents['column_mapping']['ref_range_upper']):
        dfColumns.append(Config.labevents['column_mapping']['ref_range_upper'])
        columns.append('ref_range_upper')
    if(Config.labevents['column_mapping']['flag']):
        dfColumns.append(Config.labevents['column_mapping']['flag'])
        columns.append('flag')
    if(Config.labevents['column_mapping']['priority']):
        dfColumns.append(Config.labevents['column_mapping']['priority'])
        columns.append('priority')
    if(Config.labevents['column_mapping']['comments']):
        dfColumns.append(Config.labevents['column_mapping']['comments'])
        columns.append('comments')
    Utils.saveDataframe(con=con, destinationSchemaName=sourceSchemaName, destinationTableName='labevents', columns=columns, df=df, dfColumns=dfColumns)


def importDataCsv(con, sourceSchemaName):

    if(hasattr(Config, 'patients') and 'file_name' in Config.patients and Config.patients['file_name']):
        importPatients(
            con=con,
            sourceSchemaName=sourceSchemaName,
            filePath = Config.patients['file_name'],
            fileSeparator=','
            )
    if(hasattr(Config, 'admissions') and 'file_name' in Config.admissions and Config.admissions['file_name']):
        importAdmissions(
            con=con,
            sourceSchemaName=sourceSchemaName,
            filePath = Config.admissions['file_name'],
            fileSeparator=','
            )
    if(hasattr(Config, 'chartevents') and 'file_name' in Config.chartevents and Config.chartevents['file_name']):
        importChartEvents(
            con=con,
            sourceSchemaName=sourceSchemaName,
            filePath = Config.chartevents['file_name'],
            fileSeparator=','
            )
    if(hasattr(Config, 'labevents') and 'file_name' in Config.labevents and Config.labevents['file_name']):
        importLabEvents(
            con=con,
            sourceSchemaName=sourceSchemaName,
            filePath = Config.labevents['file_name'],
            fileSeparator=','
            )
