import logging
import uuid
import boto3
import time
from threading import Thread
import requests

from dynamic_livy import constants as dlcs
from dynamic_livy.zeppelin import ZeppelinInterpreter

from dynamic_livy import pandas_lib
from dynamic_livy import pyspark_lib
from projects.models import Analysis
from projects.models import Projects, ClusterDetails
from . import constants

logger = logging.getLogger('generic.logger')

session = ZeppelinInterpreter()

pandas_pf = pandas_lib.PatientFlow()
pyspark_pf = pyspark_lib.PatientFlow()


def start_session():
    logger.info('creating livy session ')
    res = {}
    try:
        name = str(uuid.uuid4())
        cluster = ClusterDetails.objects.get(cluster_name=constants.CLUSTER_NAME)
        if dlcs.ZEPPELIN_ENVIRONMENT == "EMR":
            cluster_response = get_cluster_status(cluster.cluster_id)
            if cluster_response in ["TERMINATED", "TERMINATING", "TERMINATED_WITH_ERRORS"]:
                new_cluster = get_cluster_id()
                if not new_cluster.get('status'):
                    res['success'] = False
                    res['message'] = 'Cluster is starting'
                    return res
                x = session.create_notebook(new_cluster.get('master_ip'), name)
                session.spark_submit(new_cluster.get('master_ip'), x, "print('notebook initialization done')")
                res['success'] = True
                res['session_id'] = x
                res['name'] = name
                return res
            else:
                x = session.create_notebook(cluster.cluster_master_ip, name)
                session.spark_submit(cluster.cluster_master_ip, x, "print('notebook initialization done')")
                res['success'] = True
                res['session_id'] = x
                res['name'] = name
                return res
        else:
            x = session.create_notebook(cluster.cluster_master_ip, name)
            session.spark_submit(cluster.cluster_master_ip, x, "print('notebook initialization done')")
            res['success'] = True
            res['session_id'] = x
            res['name'] = name
            return res

    except Exception as e:
        logger.error('Error occured while creating zeppelin notebook {}'.format(e))
        res['success'] = False
        res['message'] = e
        return res


def stop_session(notebook_id):
    logger.info('deleting zeppelin notebook of id {}'.format(notebook_id))
    res = {}
    try:
        cluster = ClusterDetails.objects.get(cluster_name=constants.CLUSTER_NAME)
        session.delete_notebook(cluster.cluster_master_ip, str(notebook_id))
        res["success"] = True
        return res

    except Exception as e:
        logger.error('Error occured while deleting notebook {}'.format(e))
        res['success'] = False
        res['message'] = e
        return res


def session_status(notebook_id, prj_id):
    logger.info(
        'zeppelin notebook status of id {}'.format(notebook_id))
    res = {}
    try:
        cluster = ClusterDetails.objects.get(cluster_name=constants.CLUSTER_NAME)
        if prj_id is None:
            status = session.check_notebook_status(cluster.cluster_master_ip, str(notebook_id))
            res["success"] = status
            res["status"] = status

        else:
            session.check_notebook_status(cluster.cluster_master_ip, str(notebook_id))
            projects = Projects.objects.get(pk=prj_id)
            analysis_obj = Analysis.objects.filter(
                project__name=projects.name)

            for j in analysis_obj:
                pf = pandas_pf
                if j.analysis_data.get('sheets')[0].get('data'):
                    pf_type = "pandas"
                    if j.analysis_data.get('sheets')[0].get('data').get('lib_type'):
                        pf_type = j.analysis_data.get('sheets')[0].get('data').get('lib_type')
                    pf = pyspark_pf
                    if pf_type == "pandas":
                        pf = pandas_pf
                read_file = pf.read_file(j.analysis_data.get('id'),
                                         j.s3_path)
                session.spark_submit(cluster.cluster_master_ip, notebook_id, read_file)

            res["success"] = True
            res["status"] = True
        return res

    except Projects.DoesNotExist as e:
        logger.error('fetching project details is failed due to {}'.format(e))
        res['success'] = False
        res['message'] = 'No records are found'
        return res

    except Exception as e:
        logger.error('Error occured while fetching zeppelin notebook status '
                     '{}'.format(e))
        res['message'] = e
        res['success'] = False
        return res


def get_cluster_status(cluster_id):
    client = boto3.client("emr", region_name=constants.EMR_REGION)
    response = client.describe_cluster(ClusterId=cluster_id)
    cluster_status = response['Cluster']['Status']['State']
    return cluster_status


def get_cluster_id():
    client = boto3.client('emr', region_name=constants.EMR_REGION)
    response = client.list_clusters(ClusterStates=['WAITING', 'RUNNING'])
    cluster_id_exist = ''
    print('checking for existing cluster')
    if response['Clusters'] != []:
        for i in response['Clusters']:
            if i['Name'] == constants.CLUSTER_NAME:
                cluster_id = i['Id']
                cluster_id_exist += 'true'
                master_ip = ''
                response_1 = client.list_instances(ClusterId=cluster_id, InstanceGroupTypes=['MASTER'],
                                                   InstanceStates=['RUNNING'])
                response_1 = response_1['Instances']
                for i in response_1:
                    if 'PrivateIpAddress' in i:
                        master_ip = i['PrivateIpAddress']
                cluster = ClusterDetails.objects.get(cluster_name=constants.CLUSTER_NAME)
                cluster.cluster_id = cluster_id
                cluster.cluster_master_ip = master_ip
                cluster.save()
                url = 'http://{}:{}/api/interpreter/setting/spark'.format(master_ip, constants.ZEPPELIN_PORT)
                status = requests.get(url).json()['body']
                if status['option'].get('perNote') is None or status['option'].get('perNote') != 'scoped':
                    update_spark_interpreter(master_ip, constants.ZEPPELIN_PORT)
                return {'master_ip': master_ip, 'status': True}
    response = client.list_clusters(ClusterStates=['BOOTSTRAPPING', 'STARTING'])
    for i in response['Clusters']:
        if i['Name'] == constants.CLUSTER_NAME:
            cluster_id_exist += 'true'
            return {'status': False}
    print('Creating new cluster')
    if cluster_id_exist == '' or response['Clusters'] == []:
        Thread(target=cluster_creation, args=()).start()
        return {'status': False}


def cluster_creation():
    client = boto3.client('emr', region_name=constants.EMR_REGION)
    instance_groups = []
    instance_groups.append(
        {
            "Name": "Worker nodes",
            "Market": "ON_DEMAND",
            "InstanceRole": "CORE",
            "InstanceType": constants.CORE_INSTANCE_TYPE,
            "InstanceCount": int(constants.CORE_INSTANCE_COUNT)
        }
    )
    instance_groups.append(
        {
            "Name": "Worker nodes",
            "Market": "SPOT",
            "InstanceRole": "TASK",
            "InstanceType": constants.TASK_INSTANCE_TYPE,
            "InstanceCount": int(constants.TASK_INSTANCE_COUNT)
        }
    )
    instance_groups.append(
        {
            "Name": "master",
            "Market": "ON_DEMAND",
            "InstanceRole": "MASTER",
            "InstanceType": constants.MASTER_INSTANCE_TYPE,
            "InstanceCount": int(constants.MASTER_INSTANCE_COUNT),
        }
    )

    new_cluster_id = client.run_job_flow(
        Name=constants.CLUSTER_NAME,
        LogUri=constants.LogUri,
        ReleaseLabel=constants.ReleaseLabel,
        StepConcurrencyLevel=int(constants.STEP_CONCURRENCY_LEVEL),
        Instances={
            'InstanceGroups': instance_groups,
            'KeepJobFlowAliveWhenNoSteps': True,
            'Ec2KeyName': constants.Ec2KeyName,
            'EmrManagedSlaveSecurityGroup': constants.EmrManagedSlaveSecurityGroup,
            'EmrManagedMasterSecurityGroup': constants.EmrManagedMasterSecurityGroup,
            'ServiceAccessSecurityGroup': constants.ServiceAccessSecurityGroup,
            'Ec2SubnetId': constants.Ec2SubnetId,

        },
        BootstrapActions=[
            {
                'Name': 'Log4j Patch',
                'ScriptBootstrapAction': {
                    'Path': constants.PatchBootstrapAction
                }
            },
            {
                'Name': 'Install Dependency Packages',
                'ScriptBootstrapAction': {
                    'Path': constants.ScriptBootstrapAction
                }
            },
        ],
        Applications=[
            {'Name': 'Spark'},
            {'Name': 'Hadoop'},
            {'Name': 'Hive'},
            {'Name': 'Ganglia'},
            {'Name': 'Livy'},
            {'Name': 'Zeppelin'}
        ],
        VisibleToAllUsers=True,
        JobFlowRole=constants.JobFlowRole,
        ServiceRole=constants.ServiceRole,
        AutoScalingRole=constants.AUTO_SCALING_ROLE,
        ScaleDownBehavior=constants.SCALE_DOWN_BEHAVIOUR,
        ManagedScalingPolicy={
            "ComputeLimits": {
                "MaximumCapacityUnits": int(constants.MAX_CAPACITY_UNITS),
                "MaximumCoreCapacityUnits": int(constants.MAX_CORE_CAPACITY_UNITS),
                "MaximumOnDemandCapacityUnits": int(constants.MAX_DEMAND_CAPACITY_UNITS),
                "MinimumCapacityUnits": int(constants.MIN_CAPACITY_UNITS),
                "UnitType": constants.UNIT_TYPE
            }
        },
        Configurations=[
            {
                "Classification": "spark-env",
                "Configurations": [
                    {
                        "Classification": "export",
                        "Properties": {
                            "PYSPARK_PYTHON": "/usr/bin/python3",
                        }
                    }
                ]
            },
            {
                "Classification": "spark-defaults",
                "Properties": {
                    "spark.dynamicAllocation.enabled": constants.SPARK_DYNAMIC_ALLOCATION,
                    "spark.shuffle.service.enabled": constants.SHUFFLE_SERVICE,
                    "spark.sql.shuffle.partitions": constants.SHUFFLE_PARTITIONS,
                    "spark.default.parallelism": constants.DEFAULT_PARALLELISM,
                    "spark.executor.memory": constants.DEFAULTS_EXECUTOR_MEMORY,
                    "spark.executor.cores": constants.DEFAULTS_EXECUTOR_CORES,
                    "spark.executor.instances": constants.DEFAULTS_EXECUTOR_INSTANCES,
                    "spark.driver.memory": constants.DEFAULTS_DRIVER_MEMORY,
                    "spark.driver.cores": constants.DEFAULTS_DRIVER_CORES
                }
            },
            {
                "Classification": "yarn-site",
                "Properties": {
                    "yarn.resourcemanager.scheduler.class": "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler"
                }
            },
            {
                "Classification": "spark-hive-site",
                "Properties": {
                    "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                }
            },
            {
                "Classification": "hive-site",
                "Properties": {
                    "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                }
            },
            {
                "Classification": "zeppelin-site",
                "Properties": {
                    "zeppelin.interpreter.lifecyclemanager.class": "org.apache.zeppelin.interpreter.lifecycle.TimeoutLifecycleManager",
                    "zeppelin.interpreter.lifecyclemanager.timeout.threshold": "{}".format(constants.INTERPRETER_LIFECYCLE_THRESHOLD),
                    "zeppelin.interpreter.lifecyclemanager.timeout.checkinterval": "{}".format(constants.INTERPRETER_LIFECYCLE_CHECKINTERVAL)
                }
            },
            {
                "Classification": "hadoop-env",
                "Configurations": [
                    {
                        "Classification": "export",
                        "Properties": {
                            "JAVA_HOME": "/etc/alternatives/jre",
                            "SPARK_HOME": "/usr/lib/spark"

                        }
                    }
                ]
            },
            {
                "Classification": "zeppelin-env",
                "Properties": {
                    "PYSPARK_PYTHON": "/usr/bin/python3",
                    "JAVA_HOME": "/etc/alternatives/jre",
                    "SPARK_HOME": "/usr/lib/spark"
                },
                "Configurations": [
                    # {
                    #     "Classification": "export",
                    #     "Properties": {
                    #         "ZEPPELIN_NOTEBOOK_S3_BUCKET": "{}".format(constants.CB_OUTPUT_BUCKET),
                    #         "ZEPPELIN_NOTEBOOK_S3_USER": "dds_apps_emr_notebooks",
                    #         "ZEPPELIN_NOTEBOOK_STORAGE": "org.apache.zeppelin.notebook.repo.S3NotebookRepo"
                    #     }
                    # }
                ]
            }
        ]
    )
    cluster_id = new_cluster_id['JobFlowId']
    print(f'cluster created with id {cluster_id}')
    instance_running = 'T'
    master_ip = ''
    while True:
        time.sleep(40)
        response_1 = client.list_instances(ClusterId=cluster_id, InstanceGroupTypes=['MASTER'],
                                           InstanceStates=['RUNNING'])
        response_1 = response_1['Instances']
        print(response_1)
        if response_1 == []:
            time.sleep(30)
            continue
        elif response_1 != []:
            for i in response_1:
                if 'PrivateIpAddress' in i:
                    print(i['PrivateIpAddress'])
                    master_ip = i['PrivateIpAddress']
                elif 'PrivateIpAddress' not in i:
                    instance_running = 'F'
                    break
            if instance_running == 'F':
                continue
            else:
                break
    cluster = ClusterDetails.objects.get(cluster_name=constants.CLUSTER_NAME)
    cluster.cluster_id = cluster_id
    cluster.cluster_master_ip = master_ip
    cluster.save()
    url = 'http://{}:{}/api/interpreter/setting/spark'.format(master_ip, constants.ZEPPELIN_PORT)
    status = requests.get(url).json()['body']
    if status['option'].get('perNote') is None or status['option'].get('perNote') != 'scoped':
        update_spark_interpreter(master_ip, constants.ZEPPELIN_PORT)
    client = boto3.client("emr", region_name=constants.EMR_REGION)
    client.put_auto_termination_policy(
        ClusterId=cluster_id,
        AutoTerminationPolicy={
            'IdleTimeout': int(constants.CLUSTER_IDLE_TIMEOUT)
        }
    )
    return {'master_ip': master_ip, 'status': True}


def update_spark_interpreter(host, port):
    SPARK_INTERPRETER = constants.UAT_SPARK_INTERPRETER
    if constants.SPARK_INTERPRETER_SELECTION == "PROD":
        SPARK_INTERPRETER = constants.PROD_SPARK_INTERPRETER
    url = 'http://{}:{}/api/interpreter/setting/spark'.format(host, port)
    status = requests.delete(url)
    print('existaing Spark Interpreter deleted', status.json())
    url = 'http://{}:{}/api/interpreter/setting'.format(host, port)
    status = requests.post(url, json=SPARK_INTERPRETER)
    return status.json()

