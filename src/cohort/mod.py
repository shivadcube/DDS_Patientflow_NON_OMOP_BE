import ast
import logging
import pandas as pd
import uuid
import boto3

from datetime import datetime

from django.core.files.storage import FileSystemStorage

from cohort import constants
from services.s3 import utils
from dynamic_livy.zeppelin import ZeppelinInterpreter
from dynamic_livy import constants as dlcs
from dynamic_livy import pandas_lib
from dynamic_livy import pyspark_lib
from projects.models import ClusterDetails
from services.livy.constants import CLUSTER_NAME

zp = ZeppelinInterpreter()

pandas_pf = pandas_lib.PatientFlow()
pyspark_pf = pyspark_lib.PatientFlow()

logger = logging.getLogger('generic.logger')


def upload_cohort(request, file, session_id, analysis_id):
    logger.info('uploading cohort from {}'.format(file.name))
    res = {}
    try:
        if str(file.name).endswith('.csv'):
            cluster = ClusterDetails.objects.get(cluster_name=CLUSTER_NAME)
            fs = FileSystemStorage(location='/tmp/upload_cohort')
            file_name = uuid.uuid4().hex + '.' + file.name.split('.')[-1]
            fs.save(file_name, file)
            file_path = '/tmp/upload_cohort/' + file_name
            df = pd.read_csv(file_path, dtype=str)
            cmp_1 = {'patient_id', 'age', 'gender', 'source_value', 'specialty_code', 'code_flag'} & set(df.keys())
            cmp_2 = {'person_id', 'age', 'gender_concept_id', 'source_value', 'specialty_code', 'code_flag'} & set(df.keys())
            cmp_3 = {'patient_id', 'age', 'gender_concept_id', 'source_value', 'specialty_code', 'code_flag'} & set(df.keys())
            cmp_4 = {'person_id', 'age', 'gender', 'source_value', 'specialty_code', 'code_flag'} & set(df.keys())
            if len(cmp_1) == 6 or len(cmp_2) == 6 or len(cmp_3) == 6 or len(cmp_4) == 6:
                upload_to_s3 = utils.upload_files_to_s3(file_path, file.name)
                if upload_to_s3[2] > float(constants.COHORT_FILE_SIZE_IN_MB):
                    lib_type = 'pyspark'
                    pf = pyspark_pf
                else:
                    pf = pandas_pf
                    lib_type = 'pandas'
                read_file = pf.upload_file(analysis_id, upload_to_s3[1])
                uploaded_file = zp.spark_submit(cluster.cluster_master_ip, session_id, read_file)
                uploaded_file = ast.literal_eval(uploaded_file)
                print(uploaded_file)
                if uploaded_file['record_count'] == 0:
                    res['success'] = True
                    res['message'] = 'count_issue'
                    return res
                date_ranges = zp.spark_submit(cluster.cluster_master_ip, session_id,
                                              pf.date_range(analysis_id))
                date_ranges = ast.literal_eval(date_ranges)
                start_date = date_ranges['analysis_period'].get('start_date')
                end_date = date_ranges['analysis_period'].get('end_date')
                summary = zp.spark_submit(cluster.cluster_master_ip, session_id, pf.summary(analysis_id,
                                                                 start_date,
                                                                 end_date))
                ndc_codes = df['source_value'].fillna(0).replace(0, 'null').unique().tolist()
                res["ndc_codes"] = ndc_codes
                res['file_path'] = upload_to_s3[0]
                res['file_name'] = file.name
                res['uploaded_at'] = datetime.now().date()
                res["date_ranges"] = date_ranges
                res['cohort_summary'] = ast.literal_eval(summary)
                res['lib_type'] = lib_type
                res['success'] = True
                return res

            res["message"] = "File must have patient_id, age, gender, source_value, specialty_code and code_flag columns"
            res['success'] = False
            return res

        res["message"] = "File should be in CSV format"
        res['success'] = False
        return res

    except Exception as e:
        logger.error("fetching failed due to {}".format(e))
        res['success'] = False
        res['message'] = e
        return res


def import_cohort_summary(cohort_id, session_id, analysis_id, history_id, env_type):
    logger.info('importing  cohort summary of {}'.format(id))
    res = {}
    try:
        cluster = ClusterDetails.objects.get(cluster_name=CLUSTER_NAME)
        if env_type == "DATABRICKS":
            table_name = 'cohort_{}_{}_analytics_data'.format(cohort_id, history_id)
            db = dlcs.databricks_database
            pf = pandas_pf
            get_size = pf.get_table_size(db, table_name)
            size = eval(zp.spark_submit(cluster.cluster_master_ip, session_id, get_size))
            pf = pyspark_pf
            lib_type = 'pyspark'
            if len(size) > 0:
                size_in_bytes = int(size[0].split(' ')[0])
                size = (int(size_in_bytes) / 1024) / 1024
                pf = pyspark_pf
                lib_type = 'pyspark'
                if float(size) < float(constants.COHORT_FILE_SIZE_IN_MB):
                    pf = pandas_pf
                    lib_type = 'pandas'
            reading_data = pf.read_table(analysis_id, db, table_name)
        else:
            folder = 'cohort_{}_{}_analytics_data'.format(cohort_id, history_id)
            size = get_file_size(folder)
            if size:
                pf = pyspark_pf
                lib_type = 'pyspark'
                if size < float(constants.COHORT_FILE_SIZE_IN_MB):
                    pf = pandas_pf
                    lib_type = 'pandas'
            else:
                pf = pyspark_pf
                lib_type = 'pyspark'
            reading_data = pf.read_data(analysis_id, cohort_id, history_id)
        imported_data = zp.spark_submit(cluster.cluster_master_ip, session_id, reading_data)
        imported_data = ast.literal_eval(imported_data)
        if imported_data['record_count'] == 0:
            res['success'] = True
            res['message'] = 'count_issue'
            return res
        date_ranges = zp.spark_submit(cluster.cluster_master_ip, session_id,
                                      pf.date_range(analysis_id))
        date_ranges = ast.literal_eval(date_ranges)
        start_date = date_ranges['analysis_period'].get('start_date')
        end_date = date_ranges['analysis_period'].get('end_date')
        summary = zp.spark_submit(cluster.cluster_master_ip, session_id, pf.summary(analysis_id,
                                                         start_date,
                                                         end_date
                                                         ))

        res['success'] = True
        res['lib_type'] = lib_type
        res['cohort_summary'] = ast.literal_eval(summary)
        res['date_ranges'] = date_ranges
        res['ndc_codes'] = list(set(ast.literal_eval(summary).get('ndc_codes')))
        res['speciality_list'] = list(set(ast.literal_eval(summary).
                                          get('speciality')))
        return res

    except Exception as e:
        logger.error("fetching cohort summary failed due to {}".format(e))
        res['message'] = e
        res['success'] = False
        return res


def get_file_size(folder):
    bucket = dlcs.CB_OUTPUT_BUCKET
    output_path = dlcs.CB_OUTPUT_PATH
    key_path = output_path + '/' + folder
    s3 = boto3.client('s3')
    response = s3.list_objects(Bucket=bucket, Prefix=key_path)
    size = 0
    for content in response['Contents']:
        size += content['Size'] / 1024 / 1024
    return size
