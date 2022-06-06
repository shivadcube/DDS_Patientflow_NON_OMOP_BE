import ast
import json
import logging
import threading
from datetime import datetime
from django.forms.models import model_to_dict

from projects.models import Projects
from projects.models import Analysis
from projects.models import BackgroundJobs

from .lot_drill_down_json import LOTDrilDown

from services.s3 import utils
from services.s3 import constants as s3cs

from dynamic_livy import pandas_lib
from dynamic_livy import pyspark_lib

from dynamic_livy.zeppelin import ZeppelinInterpreter
from services.livy.mod import session_status, start_session
from projects.models import ClusterDetails
from services.livy.constants import CLUSTER_NAME

logger = logging.getLogger('generic.logger')

zp = ZeppelinInterpreter()
pandas_pf = pandas_lib.PatientFlow()
pyspark_pf = pyspark_lib.PatientFlow()


def projects_list(project_id=None, action='view'):
    logger.info('fetching projects list')
    result = []
    res = {}
    analysis_list = []
    try:
        cluster = ClusterDetails.objects.get(cluster_name=CLUSTER_NAME)
        if not project_id:
            projects = Projects.objects.all().order_by('-id')
            for i in projects:
                project_job_status_list = []
                background_job_status = BackgroundJobs.objects.filter(project=i)
                for j in background_job_status:
                    job_dict = {'status': j.status, 'analysis': j.analysis, 'attribute_type': j.attribute_type}
                    project_job_status_list.append(job_dict)
                project = {
                    'id': i.id,
                    'name': i.name,
                    'tags': ast.literal_eval(i.tags) if i.tags else [],
                    'created_by': i.created_by,
                    'metadata': json.loads(i.metadata),
                    'created_at': date_conversation(i.created_at),
                    'job_status': project_job_status_list
                }
                result.append(project)

            res['success'] = True
            res['result'] = result
            return res

        projects = Projects.objects.get(pk=int(project_id))
        project = {
            'id': projects.id,
            'name': projects.name,
            'tags': ast.literal_eval(projects.tags) if projects.tags else [],
            'created_by': projects.created_by,
            'metadata': json.loads(projects.metadata),
            'created_at': date_conversation(projects.created_at)
        }
        if action == "edit":
            status = session_status(projects.session_id, None)
            project['session_id'] = projects.session_id
            if not status.get('status'):
                new_session = start_session()
                if not new_session.get('success'):
                    return new_session
                session_status(new_session.get('session_id'), projects.id)
                project['session_id'] = new_session.get('session_id')
            projects.session_id = project['session_id']
            projects.save()
        analysis_obj = Analysis.objects.filter(
            project__name=projects.name)
        for j in analysis_obj:
            analysis = {
                'data': j.analysis_data,
                's3_path': j.s3_path,
                'lot_attributes': j.lot_attributes_id,
                'analysis_id': j.id
            }
            analysis_list.append(analysis)
        project["analyses"] = analysis_list
        background_jobs = BackgroundJobs.objects.filter(project=projects)
        job_background_status = []
        for job in background_jobs:
            job.cluster_id = cluster.cluster_id
            job.cluster_status = cluster.cluster_status
            job.zeppelin_host = cluster.cluster_master_ip
            if action == 'edit':
                job.notebook_id = project['session_id']
            job.save()

            job_background_status.append(model_to_dict(job))
        project['job_status_details'] = job_background_status
        result.append(project)

        res['success'] = True
        res['result'] = result[0]
        return res

    except Projects.DoesNotExist as e:
        logger.error('fetching project details is failed due to {}'.format(e))
        res['success'] = False
        res['message'] = 'No records are found'
        return res

    except Exception as e:
        res['success'] = False
        res['message'] = e
        return res


def create_project(request, user, params):
    logger.info('creating new project with :{}'.format(params))
    res = {}
    analysis_ids = []
    pf_dict = {}
    try:
        verify_project = Projects.objects.filter(name=params.get("name"))
        if not verify_project:
            project_obj = Projects()
            project_obj.name = params.get('name')
            project_obj.tags = params.get('tags')
            project_obj.created_by = params.get('created_by')
            project_obj.metadata = json.dumps(params.get('metadata'))
            project_obj.session_id = params.get('session_id')
            project_obj.save()

            for i in params.get('analysis'):
                if i.get('sheets')[0].get('data'):
                    if i.get('sheets')[0].get('data').get('lib_type'):
                        pf_dict[i.get('id')] = i.get('sheets')[0].get('data').get('lib_type')
                    else:
                        pf_dict[i.get('id')] = 'pandas'
                else:
                    pf_dict[i.get('id')] = "pandas"
                analysis_ids.append(i.get('id'))
                s3_path = 'prj_{}/prj_{}_{}'.format(project_obj.id,
                                                    project_obj.id,
                                                    i.get('id'))

                s3_final_path = s3cs.S3_FILE_URL_SPARK.format(s3cs.S3_PROJECT_PATH,
                                                              s3_path)
                analysis_obj = Analysis()
                analysis_obj.project = project_obj
                analysis_obj.analysis_data = i
                analysis_obj.s3_path = s3_final_path
                analysis_obj.lot_attributes_id = params.get('lot_attributes_id')
                analysis_obj.save()
            files_async = SendFilesToS3(pf_dict, project_obj.id,
                                        params.get('session_id'))
            files_async.start()

            res['success'] = True
            res['result'] = 'project created successfully'
            res["id"] = project_obj.id
            return res

        res['result'] = 'project already exist'
        return res

    except Exception as e:
        res['success'] = False
        res['message'] = e
        return res


def update_project(request, params):
    logger.info('updating existing project with :{}'.format(params))
    res = {}
    analysis_ids = []
    new_analysis_ids = []
    pf_dict = {}
    try:
        cluster = ClusterDetails.objects.get(cluster_name=CLUSTER_NAME)
        project_obj = Projects.objects.get(pk=params.get("id"))
        project_obj.tags = params.get('tags')
        project_obj.name = params.get('name')
        project_obj.created_by = params.get('created_by')
        project_obj.session_id = params.get('session_id')
        project_obj.save()

        data = Analysis.objects.filter(project__pk=params.get("id"))
        old_analysis_ids = [i.id for i in data]

        for i in params.get('analysis'):
            analysis_ids.append(i.get('id'))
            if i.get('sheets')[0].get('data'):
                if i.get('sheets')[0].get('data').get('lib_type'):
                    pf_dict[i.get('id')] = i.get('sheets')[0].get('data').get('lib_type')
                else:
                    pf_dict[i.get('id')] = 'pandas'
            else:
                pf_dict[i.get('id')] = "pandas"
            new_analysis_ids.append(i.get('analysis_id'))
            s3_path = 'prj_{}/prj_{}_{}'.format(project_obj.id,
                                                project_obj.id,
                                                i.get('id'))

            s3_final_path = s3cs.S3_FILE_URL_SPARK.format(s3cs.S3_PROJECT_PATH,
                                                          s3_path)
            analysis_data = {
                'project': project_obj,
                'analysis_data': i,
                's3_path': s3_final_path,
                'lot_attributes_id': params.get('lot_attributes_id')
            }
            Analysis.objects.update_or_create(pk=i.get('analysis_id'),
                                              defaults=analysis_data)
        diff = list(set(old_analysis_ids)-set(new_analysis_ids))
        for analysis_id in diff:
            Analysis.objects.get(pk=analysis_id).delete()
        files_async = SendFilesToS3(pf_dict, project_obj.id,
                                    params.get('session_id'))
        files_async.start()
        background_jobs = BackgroundJobs.objects.filter(project=project_obj)
        for job in background_jobs:
            job.cluster_id = cluster.cluster_id
            job.cluster_status = cluster.cluster_status
            job.zeppelin_host = cluster.cluster_master_ip
            job.notebook_id = project_obj.session_id
            job.save()

        res['success'] = True
        res['result'] = 'project updated successfully'
        return res

    except Projects.DoesNotExist as e:
        logger.error('updating project is failed due to {}'.format(e))
        res['message'] = "No records are found "
        res['success'] = False
        return res

    except Exception as e:
        logger.error('updating project is failed due to {}'.format(e))
        res['message'] = e
        res['success'] = False
        return res


def delete_projects(project_id):
    logger.info(
        "delete Projects having id and user {}".format(project_id))
    res = {}
    try:
        Projects.objects.get(pk=project_id).delete()
        utils.delete_files_from_s3()

        res['success'] = True
        res['message'] = "Project deleted successfully"
        return res

    except Projects.DoesNotExist as e:
        logger.error("Delete Project failed due to {}".format(e))
        res['success'] = False
        res['message'] = "No records are found "
        return res


def date_conversation(date):
    input_date = str(date)
    convert_date = datetime.strptime(input_date.split(".")[0],
                                     '%Y-%m-%d %H:%M:%S')
    modify_date = convert_date.date()
    return modify_date


def validate_project_name(name):
    logger.info(
        "validating Project name with the name of {}".format(name))
    res = {}
    try:
        codeset = Projects.objects.filter(name=str(name))
        if not codeset:
            res["result"] = "Project doesn't exist"
            res["success"] = True
            return res

        res["message"] = "Project already exist"
        return res

    except Exception as e:
        logger.error("Error occured while validating Project name {}".format(e))
        res['success'] = False
        res['message'] = e
        return res


def analysis_summary_sheet(request, data):
    logger.info(
        "fetching project analysis sheet summary of {} ".format(data))
    res = {}
    try:
        cluster = ClusterDetails.objects.get(cluster_name=CLUSTER_NAME)
        if data.get('lib_type', '') == 'pyspark':
            pf = pyspark_pf
        else:
            pf = pandas_pf
        attributes = data.get('attributes')
        session_id = data.get('session_id')
        analysis_id = data.get('analysis_id')
        start_date = data.get('date_ranges')['analysis_period'] \
            .get('start_date')
        end_date = data.get('date_ranges')['analysis_period'].get('end_date')
        session_id = session_id
        result = pf.analysis_summary(analysis_id,
                                     attributes,
                                     start_date,
                                     end_date)
        submit_spark = zp.spark_submit(cluster.cluster_master_ip, session_id, result)

        res["success"] = True
        spark_res = ast.literal_eval(submit_spark)
        obj = LOTDrilDown()
        res['data'] = obj.get_lot_json(spark_res)
        return res

    except Exception as e:
        res = {}
        logger.error("Error occured while fetching project analysis "
                     "sheet summary {}".format(e))
        res['message'] = e
        res['success'] = False
        return res


class SendFilesToS3(threading.Thread):

    def __init__(self, pf_dict, project_id, session_id):
        threading.Thread.__init__(self)
        self.pf_dict = pf_dict
        self.project_id = project_id
        self.session_id = session_id

    def run(self):
        logger.info(
            "async job to save files in s3 with spark of project id {} and "
            "analysis ids {}".format(self.project_id, self.pf_dict.keys()))
        cluster = ClusterDetails.objects.get(cluster_name=CLUSTER_NAME)
        for analysis_id, lib_type in self.pf_dict.items():
            if lib_type == "pyspark":
                pf = pyspark_lib.PatientFlow()
            else:
                pf = pandas_lib.PatientFlow()
            save_to_s3 = pf.save_to_s3(self.project_id, analysis_id)
            zp.spark_submit(cluster.cluster_master_ip, self.session_id, save_to_s3)
        return


def analysis_period(request, data):
    logger.info(
        "project analysis_period are {}".format(data))
    res = {}
    try:
        cluster = ClusterDetails.objects.get(cluster_name=CLUSTER_NAME)
        if data.get('lib_type', '') == 'pyspark':
            pf = pyspark_pf
        else:
            pf = pandas_pf

        session_id = data.get('session_id')
        analysis_id = data.get('analysis_id')
        start_date = data.get('date_ranges')['analysis_period'].get('start_date')
        end_date = data.get('date_ranges')['analysis_period'].get('end_date')
        s3_path = data.get('s3_path')
        session_id = session_id
        zp.spark_submit(cluster.cluster_master_ip, session_id, pf.read_file(analysis_id, s3_path))
        summary = zp.spark_submit(cluster.cluster_master_ip, session_id, pf.summary(analysis_id,
                                                         start_date,
                                                         end_date))
        res["success"] = True
        res["data"] = ast.literal_eval(summary)
        return res

    except Exception as e:
        res = {}
        logger.error("Error occured while analysis_period {}".format(e))
        res['message'] = e
        res['success'] = False
        return res


def fetch_job_status(project_id, analysis_id):
    logger.info("fetch job status with id {}".format(analysis_id))
    res = {}
    try:
        data = []
        project = Projects.objects.get(pk=project_id)
        job_status = BackgroundJobs.objects.filter(project=project, analysis=analysis_id)
        if job_status:
            for job in job_status:
                data.append(model_to_dict(job))
        res['success'] = True
        res['data'] = data
        res['message'] = "job status fetched successfully"
        return res

    except Exception as e:
        logger.error("fetching job status failed due to {}".format(e))
        res['success'] = False
        res['message'] = str(e)
        return res
