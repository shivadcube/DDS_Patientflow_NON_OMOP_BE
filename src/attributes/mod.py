import ast
import logging
import pandas as pd
from datetime import datetime
from deltalake import DeltaTable
from s3fs import S3FileSystem

from attributes import constants
from attributes.models import AgeGroupAttributes
from attributes.models import GenderAttributes
from attributes.models import ProviderSpeciality
from attributes.models import AttributeMappings
from dynamic_livy import pandas_lib
from dynamic_livy import pyspark_lib
from dynamic_livy.zeppelin import ZeppelinInterpreter
from drugs.mod import connect_to_neo4j
from projects.models import ClusterDetails, BackgroundJobs, Projects
from services.livy.constants import CLUSTER_NAME
from services.s3 import constants as s3cs
from threading import Thread

logger = logging.getLogger('generic.logger')

zp = ZeppelinInterpreter()

pandas_pf = pandas_lib.PatientFlow()
pyspark_pf = pyspark_lib.PatientFlow()
pandas_lot = pandas_lib.LOT()
pyspark_lot = pyspark_lib.LOT()
pandas_compliance = pandas_lib.Compliance()
pyspark_compliance = pyspark_lib.Compliance()
pandas_persistence = pandas_lib.Persistence()
pyspark_persistence = pyspark_lib.Persistence()


def fetch_constant_attributes():
    logger.info('fetching project constant attributes ')
    res = {}
    patient = {}
    treatment = {}
    provider = {}
    try:
        age_group = AgeGroupAttributes.objects.all().values()
        gender = GenderAttributes.objects.all().values()
        provider_speciality = ProviderSpeciality.objects.all().values()
        patient["Age Group"] = list(age_group)
        patient["Gender"] = list(gender)
        provider["Specialty"] = list(provider_speciality)
        treatment['Custom'] = constants.TREATMENT_CUSTOM

        res["attributes"] = constants.ATTRIBUTES_LIST
        res["patient"] = patient
        res['provider'] = provider
        res["treatment"] = treatment
        res["success"] = True
        return res

    except Exception as e:
        logger.error('fetching constant attributes i'
                     's failed due to {}'.format(e)
                     )
        res['success'] = False
        res['message'] = e
        return res


def attributes_mapping(data):
    logger.info('mapping attributes in csv file with livy server ')
    res = {}
    try:
        cluster = ClusterDetails.objects.get(cluster_name=CLUSTER_NAME)
        if data.get('lib_type', '') == 'pyspark':
            pf = pyspark_pf
            lot = pyspark_lot
            compliance = pyspark_compliance
            persistence = pyspark_persistence
        else:
            pf = pandas_pf
            lot = pandas_lot
            compliance = pandas_compliance
            persistence = pandas_persistence
        dimensions = data.get('dimensions')
        mappings = data.get('mappings')
        attribute_id = data.get('attribute_id', None)
        column_attribute = data.get('column_attribute')
        session_id = data.get('session_id', None)
        attribute_type = data.get('type')
        project_id = data.get("project_id")
        analysis_id = data.get("analysis_id")
        lot_mappings_json = data.get('lot_json')
        start_date = data.get('date_ranges')['analysis_period'].get('start_date')
        end_date = data.get('date_ranges')['analysis_period'].get('end_date')
        has_compliance = data.get("has_compliance")
        has_persistence = data.get("has_persistence")
        compliance_id = data.get("compliance_id")
        persistence_id = data.get("persistence_id")
        print(lot_mappings_json)

        create_attribute_mappings(data)

        if attribute_type == 'gender':
            result = pf.gender_attribute(analysis_id,
                                         mappings,
                                         dimensions,
                                         attribute_id)
            zp.spark_submit(cluster.cluster_master_ip, session_id, result)

            if has_compliance and has_persistence:
                zp.spark_submit(cluster.cluster_master_ip, session_id,
                                compliance.gender_attribute(analysis_id, mappings, dimensions, attribute_id,
                                                            project_id, compliance_id))
                zp.spark_submit(cluster.cluster_master_ip, session_id,
                                persistence.gender_attribute(analysis_id, mappings, dimensions, attribute_id,
                                                             project_id, persistence_id))
            elif has_compliance:
                zp.spark_submit(cluster.cluster_master_ip, session_id,
                                compliance.gender_attribute(analysis_id, mappings, dimensions, attribute_id,
                                                            project_id, compliance_id))
            elif has_persistence:
                zp.spark_submit(cluster.cluster_master_ip, session_id,
                                persistence.gender_attribute(analysis_id, mappings, dimensions, attribute_id,
                                                             project_id, persistence_id))

        elif attribute_type == 'age_group':
            age_dimensions = dimensions.copy()
            result = pf.age_range_attribute(analysis_id,
                                            age_dimensions,
                                            mappings,
                                            attribute_id)
            zp.spark_submit(cluster.cluster_master_ip, session_id, result)

            if has_compliance and has_persistence:
                comp_dimensions = dimensions.copy()
                persistence_dimensions = dimensions.copy()
                zp.spark_submit(cluster.cluster_master_ip, session_id,
                                compliance.age_range_attribute(analysis_id, comp_dimensions, mappings, attribute_id,
                                                               project_id,
                                                               compliance_id))
                zp.spark_submit(cluster.cluster_master_ip, session_id,
                                persistence.age_range_attribute(analysis_id, persistence_dimensions, mappings,
                                                                attribute_id,
                                                                project_id,
                                                                persistence_id))
            elif has_compliance:
                comp_dimensions = dimensions.copy()
                zp.spark_submit(cluster.cluster_master_ip, session_id,
                                compliance.age_range_attribute(analysis_id, comp_dimensions, mappings, attribute_id,
                                                               project_id,
                                                               compliance_id))
            elif has_persistence:
                persistence_dimensions = dimensions.copy()
                zp.spark_submit(cluster.cluster_master_ip, session_id,
                                persistence.age_range_attribute(analysis_id, persistence_dimensions, mappings,
                                                                attribute_id, project_id, persistence_id))

        elif attribute_type == 'drug_class':
            result = pf.generate_drug_class(analysis_id,
                                            dimensions,
                                            mappings,
                                            attribute_id)
            zp.spark_submit(cluster.cluster_master_ip, session_id, result)

        elif attribute_type == 'specialty':
            result = pf.generate_provider_specialty(analysis_id,
                                                    dimensions,
                                                    mappings,
                                                    attribute_id)
            zp.spark_submit(cluster.cluster_master_ip, session_id, result)

        elif attribute_type == 'lot':

            result = lot.create_regimen(analysis_id,
                                        lot_mappings_json,
                                        'lot_' + str(attribute_id),
                                        column_attribute, start_date, end_date)
            submit_background_job(project_id, analysis_id, session_id, 'lot', result)

        elif attribute_type == "compliance":
            compliance_mappings(data)

        elif attribute_type == 'persistence':
            persistence_mappings(data)

        res["success"] = True
        return res

    except Exception as e:
        logger.error('mapping attributes in csv file with livy server'
                     ' is failed due to {}'.format(e))
        res['success'] = False
        res['message'] = e
        return res


def attributes_drag_drop(data):
    logger.info('attributes drag and drop with livy server ')
    res = {}
    attribute_id = []
    try:
        cluster = ClusterDetails.objects.get(cluster_name=CLUSTER_NAME)
        if data.get('lib_type', '') == 'pyspark':
            pf = pyspark_pf
        else:
            pf = pandas_pf
        session_id = data.get('session_id')
        analysis_id = data.get('analysis_id')
        attribute_x = data.get('attribute_x')
        attribute_y = data.get('attribute_y')
        start_date = data.get('date_ranges')['analysis_period'].get('start_date')
        end_date = data.get('date_ranges')['analysis_period'].get('end_date')
        if attribute_x and attribute_y:
            attribute_id = [str(attribute_y), str(attribute_x)]

        elif attribute_x:
            attribute_id = [str(attribute_x)]

        elif attribute_y:
            attribute_id = [str(attribute_y)]

        result = pf.get_attribute_data(analysis_id,
                                       attribute_id,
                                       start_date,
                                       end_date)
        submit_spark = zp.spark_submit(cluster.cluster_master_ip, session_id, result)

        res["success"] = True
        res["data"] = ast.literal_eval(submit_spark)
        return res

    except Exception as e:
        logger.error('attributes drag and drop with livy server'
                     ' is failed due to {}'.format(e))
        res['success'] = False
        res['message'] = e
        return res


def fetch_specialty_attributes(data):
    logger.info('fetching project speciality attributes ')
    res = {}
    try:
        graph = connect_to_neo4j()
        if data.get('concept_ids') and len(data.get('concept_ids')) > 0:
            result = graph.run(constants.SPECIALTY_QUERY.format(data.get('concept_ids'))).data()
            res['data'] = result
            res["success"] = True
            return res
        else:
            res['data'] = []
            res['success'] = True
            return res

    except Exception as e:
        res = {}
        logger.error('speciality attributes is failed due to {}'.format(e))
        res['success'] = False
        res['message'] = e
        return res


def delete_attribute(data):
    logger.info("delete attribute with id {}".format(data.get('attribute_id')))
    res = {}
    try:
        cluster = ClusterDetails.objects.get(cluster_name=CLUSTER_NAME)
        pf = pandas_pf
        compliance = pandas_compliance
        persistence = pandas_persistence
        if data.get('lib_type', '') == 'pyspark':
            pf = pyspark_pf
            compliance = pyspark_compliance
            persistence = pyspark_persistence
        session_id = data.get('session_id')
        analysis_id = data.get('analysis_id')
        attribute_id = data.get('attribute_id')
        project_id = data.get('project_id')
        attribute_type = data.get('attribute_type')
        project = Projects.objects.get(id=int(project_id))
        AttributeMappings.objects.filter(project=project, analysis_id=analysis_id, attribute_id=attribute_id).delete()
        if attribute_type == 'lot':
            BackgroundJobs.objects.filter(project=project, analysis=analysis_id, attribute_type='lot').delete()
            result = pf.drop_attribute(analysis_id, "lot_" + attribute_id)
            zp.spark_submit(cluster.cluster_master_ip, session_id, result)
        elif attribute_type == "compliance":
            BackgroundJobs.objects.filter(project=project, analysis=analysis_id, attribute_type='compliance').delete()
            project_path = "PFA_COMPLIANCE/prj_{}_{}_{}".format(project_id, analysis_id, attribute_id)
            result = compliance.drop_attribute(project_path)
            zp.spark_submit(cluster.cluster_master_ip, session_id, result)
        elif attribute_type == "persistence":
            BackgroundJobs.objects.filter(project=project, analysis=analysis_id, attribute_type='persistence').delete()
            project_path = "PFA_PERSISTENCE/prj_{}_{}_{}".format(project_id, analysis_id, attribute_id)
            result = compliance.drop_attribute(project_path)
            zp.spark_submit(cluster.cluster_master_ip, session_id, result)
        else:
            if attribute_type == "gender" or attribute_type == "age_group":
                has_compliance = AttributeMappings.objects.filter(project=project, analysis_id=analysis_id,
                                                                  attribute_type='compliance')
                has_persistence = AttributeMappings.objects.filter(project=project, analysis_id=analysis_id,
                                                                   attribute_type='persistence')
                if has_compliance.exists() and has_persistence.exists():
                    comp_attribute_id = has_compliance.first().attribute_id
                    pers_attribute_id = has_persistence.first().attribute_id
                    zp.spark_submit(cluster.cluster_master_ip, session_id,
                                    compliance.drop_column(analysis_id, attribute_id, project_id, comp_attribute_id))
                    zp.spark_submit(cluster.cluster_master_ip, session_id,
                                    persistence.drop_column(analysis_id, attribute_id, project_id, pers_attribute_id))
                elif has_compliance.exists():
                    comp_attribute_id = has_compliance.first().attribute_id
                    zp.spark_submit(cluster.cluster_master_ip, session_id,
                                    compliance.drop_column(analysis_id, attribute_id, project_id, comp_attribute_id))
                elif has_persistence.exists():
                    pers_attribute_id = has_persistence.first().attribute_id
                    zp.spark_submit(cluster.cluster_master_ip, session_id,
                                    persistence.drop_column(analysis_id, attribute_id, project_id, pers_attribute_id))

            result = pf.drop_attribute(analysis_id, attribute_id)
            zp.spark_submit(cluster.cluster_master_ip, session_id, result)

        res['success'] = True
        res['message'] = "Attribute deleted successfully"
        return res

    except Exception as e:
        logger.error("Delete Attribute failed due to {}".format(e))
        res['success'] = False
        res['message'] = e
        return res


def gender_attribute(data):
    logger.info('fetching project gender attributes ')
    res = {}
    try:
        graph = connect_to_neo4j()
        if data.get('concept_ids') and len(data.get('concept_ids')) > 0:
            result = graph.run(constants.GENDER_QUERY.format(data.get('concept_ids'))).data()
            res['data'] = result
            res["success"] = True
            return res
        else:
            res['data'] = []
            res['success'] = True

    except Exception as e:
        res = {}
        logger.error('gender attributes is failed due to {}'.format(e))
        res['success'] = False
        res['message'] = e
        return res


def delete_analysis(project_id, analysis_id):
    logger.info("delete analysis with id {}".format(analysis_id))
    res = {}
    try:
        project = Projects.objects.get(id=int(project_id))
        BackgroundJobs.objects.filter(project=project, analysis=analysis_id).delete()
        res['success'] = True
        res['message'] = "Analysis deleted successfully"
        return res

    except Exception as e:
        logger.error("Delete Analysis failed due to {}".format(e))
        res['success'] = False
        res['message'] = str(e)
        return res


def get_project_name(project_id):
    try:
        project = Projects.objects.get(id=int(project_id))
        if project:
            return project.name
        return ""
    except Projects.DoesNotExist:
        logger.error("project not found")
        return ""
    except Exception as e:
        logger.error("Error happend while fetching project name :{}".format(e))
        return ""


def output_data_download(project_id, analysis_id, attribute_id, session_id, code_type, lib_type):
    logger.info("download the output data for analysis_id: {}".format(analysis_id))
    res = {}
    try:
        cluster = ClusterDetails.objects.get(cluster_name=CLUSTER_NAME)
        if lib_type == 'pyspark':
            pf = pyspark_pf
        else:
            pf = pandas_pf
        s3_path = s3cs.S3_PROJECT_PATH
        if code_type == "lot":
            save_file = pf.save_to_s3(project_id, analysis_id)
            zp.spark_submit(cluster.cluster_master_ip, session_id, save_file)
            filename = 'prj_{0}/prj_{0}_{1}'.format(project_id, analysis_id)
        elif code_type == "compliance":
            filename = 'PFA_COMPLIANCE/prj_{0}_{1}_{2}'.format(project_id, analysis_id, attribute_id)
        else:
            filename = 'PFA_PERSISTENCE/prj_{0}_{1}_{2}'.format(project_id, analysis_id, attribute_id)
        if lib_type == 'pyspark':
            data = DeltaTable(s3_path + "/" + filename, file_system=S3FileSystem()).to_pandas()
        else:
            data = pd.read_parquet("s3://{}/{}".format(s3_path, filename))
        result = rename_output_csv_header(data, project_id, analysis_id)
        res['success'] = True
        res['results'] = result['data']
        return res

    except Exception as e:
        logger.error("Data download for analysis failed due to {}".format(e))
        res['success'] = False
        res['message'] = str(e)
        return res


def generated_code_download(data):
    logger.info("download the code for analysis_id: {}".format(data.get('analysis_id')))
    res = {}
    try:
        project_id = data.get('project_id')
        analysis_id = data.get('analysis_id')
        code_type = data.get('code_type')
        project_name = get_project_name(int(project_id))
        file_name = "{}_{}_{}_{}.py".format(project_name, project_id, analysis_id, code_type)
        output_file_path = '/tmp/{}'.format(file_name)
        file = open(output_file_path, "w")

        if code_type == "lot_summary":
            code_list = generate_lot_summary_code_list(data)
        elif code_type == "compliance_summary":
            code_list = generate_compliance_summary_code_list(data)
        elif code_type == "persistence_summary":
            code_list = generate_persistence_summary_code_list(data)
        else:
            code_list = generate_code_list(data)

        final_code = "\n\n\n".join(code_list)
        file.write(final_code)
        file.close()

        res['success'] = True
        res['results'] = output_file_path
        res['file_name'] = file_name
        return res

    except Exception as e:
        logger.error("Code download for analysis failed due to {}".format(e))
        res['success'] = False
        res['message'] = str(e)
        return res


def generate_lot_summary_code_list(data):
    graph_code_list = []
    try:
        if data.get('lib_type', '') == 'pyspark':
            pf = pyspark_pf
            lot = pyspark_lot
        else:
            pf = pandas_pf
            lot = pandas_lot
        project_id = data.get('project_id')
        analysis_id = data.get('analysis_id')
        start_date = data.get('date_ranges')['analysis_period'].get('start_date')
        end_date = data.get('date_ranges')['analysis_period'].get('end_date')
        attribute_id = data.get('attribute_id')
        lot_attributes = data.get('lot_attributes', {})
        line_number = data.get('line_number', 1)
        regimen = data.get('regimen', "")

        comment_line = "\n#################################### {} ####################################\n"
        s3_path = 'prj_{0}/prj_{0}_{1}'.format(project_id, analysis_id)
        s3_final_path = s3cs.S3_FILE_URL_SPARK.format(s3cs.S3_PROJECT_PATH, s3_path)

        read_file_code = pf.read_file(analysis_id, s3_final_path)
        graph_code_list.append(comment_line.format("Start of Read File"))
        graph_code_list.append(read_file_code)
        graph_code_list.append(comment_line.format("End of Read File"))

        graph_ab = lot.patient_share(analysis_id,
                                     'lot_' + str(attribute_id),
                                     start_date,
                                     end_date, lot_attributes
                                     )
        graph_code_list.append(graph_ab)

        graph_c = lot.deep_dive_lot_regimen(analysis_id,
                                            'lot_' + str(attribute_id),
                                            start_date,
                                            end_date, None, None, lot_attributes
                                            )
        graph_code_list.append(comment_line.format("Start of Patient Share by Regimens Across Line of Therapy"))
        graph_code_list.append(graph_c)
        graph_code_list.append(comment_line.format("End of Patient Share by Regimens Across Line of Therapy"))

        graph_d = lot.deep_dive_lot_regimen(analysis_id,
                                            'lot_' + str(attribute_id),
                                            start_date,
                                            end_date, line_number, regimen, lot_attributes
                                            )
        graph_code_list.append(comment_line.format("Start of Line and Regimen Deep Dive"))
        graph_code_list.append(graph_d)
        graph_code_list.append(comment_line.format("End of Line and Regimen Deep Dive"))

        graph_e = lot.regimen_combomono_share(analysis_id,
                                              'lot_' + str(attribute_id),
                                              start_date,
                                              end_date, lot_attributes
                                              )
        graph_code_list.append(comment_line.format("Start of Share of Mono and Combo Regimens Across Line"))
        graph_code_list.append(graph_e)
        graph_code_list.append(comment_line.format("End of Share of Mono and Combo Regimens Across Line"))

        graph_f = lot.regimen_drugs_share(analysis_id,
                                          'lot_' + str(attribute_id),
                                          start_date,
                                          end_date,
                                          line_number,
                                          lot_attributes,
                                          "mono",
                                          )
        graph_code_list.append(comment_line.format("Start of Top Regimens - Line 1, Mono"))
        graph_code_list.append(graph_f)
        graph_code_list.append(comment_line.format("End of Top Regimens - Line 1, Mono"))

        graph_gh = lot.average_treatment_days(analysis_id,
                                              'lot_' + str(attribute_id),
                                              start_date,
                                              end_date, lot_attributes)
        graph_code_list.append(comment_line.format("Start of Average Treatment Days Across Line of Therapy and "
                                                   "Regimens"))
        graph_code_list.append(graph_gh)
        graph_code_list.append(comment_line.format("End of Average Treatment Days Across Line of Therapy and "
                                                   "Regimens"))

        return graph_code_list

    except Exception as e:
        graph_code_list.append(e)
        return graph_code_list


def generate_compliance_summary_code_list(data):
    graph_code_list = []
    try:
        if data.get('lib_type', '') == 'pyspark':
            compliance = pyspark_compliance
        else:
            compliance = pandas_compliance
        project_id = data.get('project_id')
        analysis_id = data.get('analysis_id')
        attribute_id = data.get('attribute_id')
        compliance_json = data.get('compliance_json', {})

        comment_line = "\n#################################### {} ####################################\n"

        read_file_code = compliance.read_file(project_id, analysis_id, attribute_id)
        graph_code_list.append(comment_line.format("Start Of Read File"))
        graph_code_list.append(read_file_code)
        graph_code_list.append(comment_line.format("End Of Read File"))

        graph_a = compliance.average_compliance(analysis_id, compliance_json)

        graph_code_list.append(comment_line.format("Start Of Average Compliance Rate By Product"))
        graph_code_list.append(graph_a)
        graph_code_list.append(comment_line.format("End Of Average Compliance Rate By Product"))

        graph_b = compliance.average_compliance_over_time(analysis_id, "PRODUCT NAME", ["YEAR LIST"], 'month')

        graph_code_list.append(
            comment_line.format("Start Of Average Compliance Rate Of The Selected Product Over Time"))
        graph_code_list.append(graph_b)
        graph_code_list.append(comment_line.format("End Of Average Compliance Rate Of The Selected Product Over Time"))

        graph_c = compliance.compliant_patients(analysis_id)

        graph_code_list.append(comment_line.format("Start Of Percentage Patient Distribution By Product And Segment"))
        graph_code_list.append(graph_c)
        graph_code_list.append(comment_line.format("End Of Percentage Patient Distribution By Product And Segment"))

        graph_d = compliance.compliant_patients_over_time(analysis_id, "PRODUCT NAME", ["YEAR LIST"], 'month')

        graph_code_list.append(comment_line.format("Start Of Percentage Patient Distribution Of The Selected Product"
                                                   " With In Segment Over Time"))
        graph_code_list.append(graph_d)
        graph_code_list.append(comment_line.format("End Of Percentage Patient Distribution Of The Selected Product With"
                                                   " In Segment Over Time"))

        graph_e = compliance.percentage_patient_distribution(analysis_id)

        graph_code_list.append(comment_line.format("Start Of Percentage Patient Distribution By Product And "
                                                   "Compliance Rate"))
        graph_code_list.append(graph_e)
        graph_code_list.append(comment_line.format("End Of Percentage Patient Distribution By Product And Compliance "
                                                   "Rate"))

        graph_f = compliance.patient_distribution(analysis_id, "PRODUCT NAME")

        graph_code_list.append(comment_line.format("Start Of Percentage Patient Distribution Of The Selected Product "
                                                   "Over Compliance Rate"))
        graph_code_list.append(graph_f)
        graph_code_list.append(comment_line.format("End Of Percentage Patient Distribution Of The Selected Product "
                                                   "Over Compliance Rate"))

        return graph_code_list

    except Exception as e:
        graph_code_list.append(e)
        return graph_code_list


def generate_persistence_summary_code_list(data):
    graph_code_list = []
    try:
        if data.get('lib_type', '') == 'pyspark':
            persistence = pyspark_persistence
        else:
            persistence = pandas_persistence
        project_id = data.get('project_id')
        analysis_id = data.get('analysis_id')
        attribute_id = data.get('attribute_id')
        persistence_json = data.get('persistence_json', {})

        comment_line = "\n#################################### {} ####################################\n"

        read_file_code = persistence.read_file(project_id, analysis_id, attribute_id)
        graph_code_list.append(comment_line.format("Start Of Read File"))
        graph_code_list.append(read_file_code)
        graph_code_list.append(comment_line.format("End Of Read File"))

        graph_a = persistence.patient_level_graph(analysis_id, persistence_json)

        graph_code_list.append(comment_line.format("Start Of Percentage Patient Distribution By Product And "
                                                   "Persistence Rate"))
        graph_code_list.append(graph_a)
        graph_code_list.append(comment_line.format("End Of Percentage Patient Distribution By Product And Persistence "
                                                   "Rate"))

        return graph_code_list

    except Exception as e:
        graph_code_list.append(e)
        return graph_code_list


def generate_code_list(data):
    code_list = []
    try:
        if data.get('lib_type', '') == 'pyspark':
            pf = pyspark_pf
            lot = pyspark_lot
            compliance = pyspark_compliance
            persistence = pyspark_persistence
        else:
            pf = pandas_pf
            lot = pandas_lot
            compliance = pandas_compliance
            persistence = pandas_persistence
        project_id = data.get('project_id')
        analysis_id = data.get('analysis_id')
        start_date = data.get('date_ranges')['analysis_period'].get('start_date')
        end_date = data.get('date_ranges')['analysis_period'].get('end_date')
        attribute_list = data.get('attribute_list')

        comment_line = "\n#################################### {} ####################################\n"
        s3_path = 'prj_{0}/prj_{0}_{1}'.format(project_id, analysis_id)
        s3_final_path = s3cs.S3_FILE_URL_SPARK.format(s3cs.S3_PROJECT_PATH, s3_path)

        read_file_code = pf.read_file(analysis_id, s3_final_path)
        code_list.append(comment_line.format("Start of Read File"))
        code_list.append(read_file_code)
        code_list.append(comment_line.format("End of Read File"))

        for attribute in attribute_list:
            if attribute.get('type') == 'gender':
                result = pf.gender_attribute(analysis_id,
                                             attribute.get('dimensions'),
                                             attribute.get('mappings'),
                                             attribute.get('attribute_id'))
                code_list.append(comment_line.format("Start of Gender Attribute"))
                code_list.append(result)
                code_list.append(comment_line.format("End of Gender Attribute"))

            elif attribute.get('type') == 'age_group':
                attribute.get('dimensions').pop()
                result = pf.age_range_attribute(analysis_id,
                                                attribute.get('dimensions'),
                                                attribute.get('mappings'),
                                                attribute.get('attribute_id'))
                code_list.append(comment_line.format("Start of Age Attribute"))
                code_list.append(result)
                code_list.append(comment_line.format("End of Age Attribute"))

            elif attribute.get('type') == 'custom':
                result = pf.generate_drug_class(analysis_id,
                                                attribute.get('dimensions'),
                                                attribute.get('mappings'),
                                                attribute.get('attribute_id'))
                code_list.append(comment_line.format("Start of Drug Attribute"))
                code_list.append(result)
                code_list.append(comment_line.format("End of Drug Attribute"))

            elif attribute.get('type') == 'specialty':
                result = pf.generate_provider_specialty(analysis_id,
                                                        attribute.get('dimensions'),
                                                        attribute.get('mappings'),
                                                        attribute.get('attribute_id'))
                code_list.append(comment_line.format("Start of Specialty Attribute"))
                code_list.append(result)
                code_list.append(comment_line.format("End of Specialty Attribute"))

            elif attribute.get('type') == 'lot':
                lot_mappings_json = attribute.get('lot_json')
                column_attribute = attribute.get('column_attribute')
                result = lot.create_regimen(analysis_id,
                                            lot_mappings_json,
                                            'lot_' + str(attribute.get('attribute_id')),
                                            column_attribute, start_date, end_date)
                code_list.append(comment_line.format("Start of Line of Therapy"))
                code_list.append(result)
                code_list.append(comment_line.format("End of Line of Therapy"))

            elif attribute.get('type') == 'compliance':
                compliance_mappings_json = attribute.get('compliance_mappings_json')
                attribute_id = attribute.get('attribute_id')
                if compliance_mappings_json.get('Days_of_Supply_Type') == "Treating_dos":
                    for item in compliance_mappings_json.get('Days_of_Supply_Details').get('Treat_dos_json'):
                        item.pop('code_type', None)
                        item.pop('code_description', None)
                        item.pop('product_name', None)
                compliance_mappings_json['Time_period']['Value'] = \
                    compliance_mappings_json['Time_period']['Value'].split('T')[
                        0] + ' 00:00:00'
                if compliance_mappings_json.get('Time_period').get('Extent'):
                    compliance_mappings_json['Time_period']['Extent'] = \
                        compliance_mappings_json['Time_period']['Extent'].split('T')[0] + ' 00:00:00'

                unwanted_attributes = AttributeMappings.objects.filter(project__id=project_id).filter(
                    analysis_id=analysis_id). \
                    filter(attribute_type__in=['specialty', 'drug_class']).values_list('attribute_id', flat=True)

                compliance_result = compliance.compliance_metric(analysis_id,
                                                                 [compliance_mappings_json],
                                                                 start_date, end_date, unwanted_attributes)
                save_to_s3 = compliance.save_to_s3(project_id, analysis_id, attribute_id)
                result = compliance_result + "\n" + save_to_s3
                code_list.append(comment_line.format("Start of Compliance Attribute"))
                code_list.append(result)
                code_list.append(comment_line.format("End of Compliance Attribute"))

            elif attribute.get('type') == 'persistence':
                persistence_mappings_json = attribute.get('persistence_mappings_json')
                attribute_id = attribute.get('attribute_id')
                if persistence_mappings_json.get('Days_of_Supply_Type') == "Treating_dos":
                    for item in persistence_mappings_json.get('Days_of_Supply_Details').get('Treat_dos_json'):
                        item.pop('code_type', None)
                        item.pop('code_description', None)
                        item.pop('product_name', None)
                persistence_mappings_json['Time_period']['Value'] = \
                    persistence_mappings_json['Time_period']['Value'].split('T')[
                        0] + ' 00:00:00'
                if persistence_mappings_json.get('Time_period').get('Extent'):
                    persistence_mappings_json['Time_period']['Extent'] = \
                        persistence_mappings_json['Time_period']['Extent'].split('T')[0] + ' 00:00:00'

                unwanted_attributes = AttributeMappings.objects.filter(project__id=project_id).filter(
                    analysis_id=analysis_id). \
                    filter(attribute_type__in=['specialty', 'drug_class']).values_list('attribute_id', flat=True)

                persistence_result = persistence.persistence_metric(analysis_id,
                                                                    [persistence_mappings_json],
                                                                    start_date, end_date, unwanted_attributes)
                save_to_s3 = persistence.save_to_s3(project_id, analysis_id, attribute_id)
                result = persistence_result + "\n" + save_to_s3
                code_list.append(comment_line.format("Start of Persistence Attribute"))
                code_list.append(result)
                code_list.append(comment_line.format("End of Persistence Attribute"))

        save_file_code = pf.save_to_s3(project_id, analysis_id)
        code_list.append(comment_line.format("Start of Save File"))
        code_list.append(save_file_code)
        code_list.append(comment_line.format("End of Save File"))

        return code_list

    except Exception as e:
        code_list.append(e)
        return code_list


def fetch_data_for_drug_codes(data):
    logger.info('fetching data for drug codes')
    res = {}
    try:
        graph = connect_to_neo4j()
        if len(data) > 0:
            query = constants.DRUG_CODE_OUTPUT_QUERY.format([code.get('source_value') for code in data])
            result = graph.run(query).data()
            for code_1 in data:
                for code_2 in result:
                    if code_1.get('source_value') == code_2.get('source_value'):
                        code_2['mean_dos_value'] = code_1['mean_dos_value']
                        code_2['default_dos_value'] = ''
                        code_2['product_name'] = code_1['product_name']
                        break
            res['data'] = result
            res["success"] = True
            return res
        else:
            res['data'] = []
            res['success'] = True
            return res

    except Exception as e:
        res = {}
        logger.error('fetching data for drug codes is failed due to {}'.format(e))
        res['success'] = False
        res['message'] = e
        return res


def compliance_mappings(data):
    logger.info('compliance mappings has been initiated')
    res = {}
    try:
        if data.get('lib_type', '') == 'pyspark':
            compliance = pyspark_compliance
        else:
            compliance = pandas_compliance
        project_id = data.get('project_id')
        attribute_id = data.get('attribute_id', None)
        session_id = data.get('session_id', None)
        analysis_id = data.get("analysis_id")
        compliance_mappings_json = data.get('compliance_mappings_json')
        start_date = data.get('date_ranges')['analysis_period'].get('start_date')
        end_date = data.get('date_ranges')['analysis_period'].get('end_date')

        if compliance_mappings_json.get('Days_of_Supply_Type') == "Treating_dos":
            for item in compliance_mappings_json.get('Days_of_Supply_Details').get('Treat_dos_json'):
                item.pop('code_type', None)
                item.pop('code_description', None)
                item.pop('product_name', None)
        compliance_mappings_json['Time_period']['Value'] = compliance_mappings_json['Time_period']['Value'].split('T')[
                                                               0] + ' 00:00:00'
        if compliance_mappings_json.get('Time_period').get('Extent'):
            compliance_mappings_json['Time_period']['Extent'] = \
                compliance_mappings_json['Time_period']['Extent'].split('T')[0] + ' 00:00:00'

        unwanted_attributes = AttributeMappings.objects.filter(project__id=project_id).filter(analysis_id=analysis_id). \
            filter(attribute_type__in=['specialty', 'drug_class']).values_list('attribute_id', flat=True)

        compliance_result = compliance.compliance_metric(analysis_id,
                                                         [compliance_mappings_json],
                                                         start_date, end_date, unwanted_attributes)
        save_to_s3 = compliance.save_to_s3(project_id, analysis_id, attribute_id)
        result = compliance_result + "\n" + save_to_s3
        submit_background_job(project_id, analysis_id, session_id, 'compliance', result)
        res["success"] = True
        return res

    except Exception as e:
        logger.error('compliance mappings is failed due to {}'.format(e))
        res['success'] = False
        res['message'] = e
        return res


def persistence_mappings(data):
    logger.info('persistence mappings has been initiated')
    res = {}
    try:
        if data.get('lib_type', '') == 'pyspark':
            persistence = pyspark_persistence
        else:
            persistence = pandas_persistence
        project_id = data.get('project_id')
        attribute_id = data.get('attribute_id', None)
        session_id = data.get('session_id', None)
        analysis_id = data.get("analysis_id")
        persistence_mappings_json = data.get('persistence_mappings_json')
        start_date = data.get('date_ranges')['analysis_period'].get('start_date')
        end_date = data.get('date_ranges')['analysis_period'].get('end_date')

        if persistence_mappings_json.get('Days_of_Supply_Type') == "Treating_dos":
            for item in persistence_mappings_json.get('Days_of_Supply_Details').get('Treat_dos_json'):
                item.pop('code_type', None)
                item.pop('code_description', None)
                item.pop('product_name', None)
        persistence_mappings_json['Time_period']['Value'] = \
            persistence_mappings_json['Time_period']['Value'].split('T')[
                0] + ' 00:00:00'
        if persistence_mappings_json.get('Time_period').get('Extent'):
            persistence_mappings_json['Time_period']['Extent'] = \
                persistence_mappings_json['Time_period']['Extent'].split('T')[0] + ' 00:00:00'

        unwanted_attributes = AttributeMappings.objects.filter(project__id=project_id).filter(analysis_id=analysis_id). \
            filter(attribute_type__in=['specialty', 'drug_class']).values_list('attribute_id', flat=True)

        persistence_result = persistence.persistence_metric(analysis_id,
                                                            [persistence_mappings_json],
                                                            start_date, end_date, unwanted_attributes)
        save_to_s3 = persistence.save_to_s3(project_id, analysis_id, attribute_id)
        result = persistence_result + "\n" + save_to_s3
        submit_background_job(project_id, analysis_id, session_id, 'persistence', result)
        res["success"] = True
        return res

    except Exception as e:
        logger.error('persistence mappings is failed due to {}'.format(e))
        res['success'] = False
        res['message'] = e
        return res


def get_drug_codes_table(data):
    logger.info('To get the drug codes table')
    res = {}
    try:
        cluster = ClusterDetails.objects.get(cluster_name=CLUSTER_NAME)
        if data.get('lib_type', '') == 'pyspark':
            compliance = pyspark_compliance
        else:
            compliance = pandas_compliance
        session_id = data.get('session_id', None)
        analysis_id = data.get("analysis_id")
        column_attribute = data.get('column_attribute')
        product_list = data.get('product_list')
        start_date = data.get('date_ranges')['analysis_period'].get('start_date')
        end_date = data.get('date_ranges')['analysis_period'].get('end_date')

        result = compliance.create_mean_dos_table(analysis_id, column_attribute, product_list, start_date, end_date)
        submit_spark = zp.spark_submit(cluster.cluster_master_ip, session_id, result)
        output_data = fetch_data_for_drug_codes(ast.literal_eval(submit_spark))
        res["success"] = True
        res["data"] = output_data.get('data')
        return res

    except Exception as e:
        res = {}
        logger.error('fetching drug codes table is failed due to {}'.format(e))
        res['success'] = False
        res['message'] = e
        return res


def generate_compliance_graph(data):
    logger.info('To generate compliance graphs')
    res = {}
    graph_data = {}
    try:
        cluster = ClusterDetails.objects.get(cluster_name=CLUSTER_NAME)
        if data.get('lib_type', '') == 'pyspark':
            compliance = pyspark_compliance
        else:
            compliance = pandas_compliance
        session_id = data.get('session_id', None)
        project_id = data.get('project_id')
        analysis_id = data.get("analysis_id")
        attribute_id = data.get('attribute_id')
        compliance_json = data.get('compliance_json', {})

        read_file = compliance.read_file(project_id, analysis_id, attribute_id)
        zp.spark_submit(cluster.cluster_master_ip, session_id, read_file)

        result_a = compliance.average_compliance(analysis_id, compliance_json)
        submit_a = zp.spark_submit(cluster.cluster_master_ip, session_id, result_a)
        graph_a_response = ast.literal_eval(submit_a)

        average_yr_list = compliance.years_list_1b(analysis_id, graph_a_response.get('graph')[0].get('product'))
        submit_avg_yr_list = ast.literal_eval(zp.spark_submit(cluster.cluster_master_ip, session_id, average_yr_list))

        result_b = compliance.average_compliance_over_time(analysis_id, graph_a_response.get('graph')[0].get('product'),
                                                           [submit_avg_yr_list[0]], 'month')
        submit_b = zp.spark_submit(cluster.cluster_master_ip, session_id, result_b)

        result_c = compliance.compliant_patients(analysis_id)
        submit_c = zp.spark_submit(cluster.cluster_master_ip, session_id, result_c)
        graph_c_response = ast.literal_eval(submit_c)

        patient_yr_list = compliance.years_list_2b(analysis_id, graph_c_response[0].get('product'))
        submit_patient_yr_list = ast.literal_eval(
            zp.spark_submit(cluster.cluster_master_ip, session_id, patient_yr_list))

        result_d = compliance.compliant_patients_over_time(analysis_id, graph_c_response[0].get('product'),
                                                           [submit_patient_yr_list[0]], 'month')
        submit_d = zp.spark_submit(cluster.cluster_master_ip, session_id, result_d)

        result_e = compliance.percentage_patient_distribution(analysis_id)
        submit_e = zp.spark_submit(cluster.cluster_master_ip, session_id, result_e)
        graph_e_response = ast.literal_eval(submit_e)

        product_f = list(graph_e_response.get('graph')[0].keys())[1]
        result_f = compliance.patient_distribution(analysis_id, product_f)
        submit_f = zp.spark_submit(cluster.cluster_master_ip, session_id, result_f)

        graph_data['avg_compliance'] = graph_a_response.get('graph')
        graph_data['avg_compliance_over_time'] = ast.literal_eval(submit_b).get('graph')
        graph_data['compliant_patients'] = graph_c_response
        graph_data['compliant_patients_over_time'] = ast.literal_eval(submit_d).get('graph')
        graph_data['concentration_curve'] = graph_e_response.get('graph')
        graph_data['histogram'] = ast.literal_eval(submit_f).get('graph')
        graph_data['dependant_keys'] = {"avg_compliance": {'product': graph_a_response.get('graph')[0].get('product'),
                                                           'period': 'month',
                                                           'selected_year': [submit_avg_yr_list[0]],
                                                           'year_list': submit_avg_yr_list},
                                        "compliant_patients": {'product': graph_c_response[0].get('product'),
                                                               'compliance_flag': 'Compliant',
                                                               'period': 'month',
                                                               'selected_year': [submit_patient_yr_list[0]],
                                                               'year_list': submit_patient_yr_list,
                                                               'patient_count_by_year': ast.literal_eval(submit_d).get(
                                                                   'patient_count_by_year')},
                                        "histogram": {"product": product_f}}
        graph_data['gt_150_patients'] = {"graph_1": graph_a_response.get('gt_150_patients'), "graph_2":
            ast.literal_eval(submit_b).get('gt_150_patients'), "graph_3":
                                             graph_e_response.get('gt_150_patients'), "graph_4":
                                             ast.literal_eval(submit_f).get('gt_150_patients')}
        res["success"] = True
        res["data"] = graph_data
        return res

    except Exception as e:
        res = {}
        logger.error('generating compliance graph is failed due to {}'.format(e))
        res['success'] = False
        res['message'] = e
        return res


def generate_persistence_graph(data):
    logger.info('To generate persistence graphs')
    res = {}
    graph_data = {}
    try:
        cluster = ClusterDetails.objects.get(cluster_name=CLUSTER_NAME)
        if data.get('lib_type', '') == 'pyspark':
            persistence = pyspark_persistence
        else:
            persistence = pandas_persistence
        session_id = data.get('session_id', None)
        project_id = data.get('project_id')
        analysis_id = data.get("analysis_id")
        attribute_id = data.get('attribute_id')
        persistence_json = data.get('persistence_json', {})

        read_file = persistence.read_file(project_id, analysis_id, attribute_id)
        zp.spark_submit(cluster.cluster_master_ip, session_id, read_file)

        result_a = persistence.patient_level_graph(analysis_id, persistence_json)
        submit_a = zp.spark_submit(cluster.cluster_master_ip, session_id, result_a)

        graph_data['patient_level_graph'] = ast.literal_eval(submit_a)
        res["success"] = True
        res["data"] = graph_data
        return res

    except Exception as e:
        res = {}
        logger.error('generating persistence graph is failed due to {}'.format(e))
        res['success'] = False
        res['message'] = e
        return res


def average_compliance_over_time(data):
    logger.info('To Generate Average Compliance Over Time Graph')
    res = {}
    try:
        cluster = ClusterDetails.objects.get(cluster_name=CLUSTER_NAME)
        if data.get('lib_type', '') == 'pyspark':
            compliance = pyspark_compliance
        else:
            compliance = pandas_compliance
        session_id = data.get('session_id', None)
        analysis_id = data.get("analysis_id")
        product = data.get('product')
        selected_year = data.get('selected_year')
        period = data.get('period')

        avg_comp_over_time = compliance.average_compliance_over_time(analysis_id, product, selected_year, period)
        submit_avg_comp_over_time = zp.spark_submit(cluster.cluster_master_ip, session_id, avg_comp_over_time)

        average_yr_list = compliance.years_list_1b(analysis_id, product)
        submit_avg_yr_list = ast.literal_eval(zp.spark_submit(cluster.cluster_master_ip, session_id, average_yr_list))

        res["success"] = True
        res["data"] = ast.literal_eval(submit_avg_comp_over_time)
        res["data"]['year_list'] = submit_avg_yr_list
        return res

    except Exception as e:
        res = {}
        logger.error('Average Compliance Over Time Graph is failed due to {}'.format(e))
        res['success'] = False
        res['message'] = e
        return res


def compliant_patients_over_time(data):
    logger.info('To Generate Compliant Patients Over Time Graph')
    res = {}
    try:
        cluster = ClusterDetails.objects.get(cluster_name=CLUSTER_NAME)
        if data.get('lib_type', '') == 'pyspark':
            compliance = pyspark_compliance
        else:
            compliance = pandas_compliance
        session_id = data.get('session_id', None)
        analysis_id = data.get("analysis_id")
        product = data.get('product')
        selected_year = data.get('selected_year')
        period = data.get('period')

        comp_patient_over_time = compliance.compliant_patients_over_time(analysis_id, product,
                                                                         selected_year, period)
        submit_comp_patient_over_time = zp.spark_submit(cluster.cluster_master_ip, session_id, comp_patient_over_time)

        res["success"] = True
        res["data"] = ast.literal_eval(submit_comp_patient_over_time).get('graph')
        return res

    except Exception as e:
        res = {}
        logger.error('Compliant Patients Over Time Graph is failed due to {}'.format(e))
        res['success'] = False
        res['message'] = e
        return res


def compliance_histogram_graph(data):
    logger.info('To Generate Compliance Histogram Graph')
    res = {}
    try:
        cluster = ClusterDetails.objects.get(cluster_name=CLUSTER_NAME)
        if data.get('lib_type', '') == 'pyspark':
            compliance = pyspark_compliance
        else:
            compliance = pandas_compliance
        session_id = data.get('session_id', None)
        analysis_id = data.get("analysis_id")
        product = data.get('product')

        compliance_histogram = compliance.patient_distribution(analysis_id, product)
        submit_compliance_histogram = zp.spark_submit(cluster.cluster_master_ip, session_id, compliance_histogram)

        res["success"] = True
        res["data"] = ast.literal_eval(submit_compliance_histogram)
        return res

    except Exception as e:
        res = {}
        logger.error('Compliance Histogram Graph is failed due to {}'.format(e))
        res['success'] = False
        res['message'] = e
        return res


def submit_background_job(project_id, analysis_id, session_id, attribute_type, result):
    logger.info('To submit background jobs initiated')
    res = {}
    try:
        cluster = ClusterDetails.objects.get(cluster_name=CLUSTER_NAME)
        project = Projects.objects.get(id=int(project_id))
        record_check = BackgroundJobs.objects.filter(project=project).filter(analysis=analysis_id).filter(
            attribute_type=attribute_type)
        paragraph_id = zp.append_paragraph(cluster.cluster_master_ip, session_id, result)
        if record_check:
            background_record = BackgroundJobs.objects.get(project=project, analysis=analysis_id,
                                                           attribute_type=attribute_type)
            background_record.notebook_id = session_id
            background_record.paragraph_id = paragraph_id
            background_record.cluster_id = cluster.cluster_id
            background_record.cluster_status = cluster.cluster_status
            background_record.zeppelin_host = cluster.cluster_master_ip
            background_record.created_at = datetime.now()
            background_record.status = "READY"
            background_record.save()
        else:
            BackgroundJobs.objects.create(project=project, analysis=analysis_id, status="READY",
                                          cluster_id=cluster.cluster_id, cluster_status=cluster.cluster_status,
                                          zeppelin_host=cluster.cluster_master_ip, notebook_id=session_id,
                                          paragraph_id=paragraph_id, attribute_type=attribute_type)
        Thread(target=zp.spark_job_submit, args=(cluster.cluster_master_ip, session_id, paragraph_id,)).start()
        res["success"] = True
        res["message"] = "{} job has been submitted successfully".format(attribute_type)
        return res

    except Exception as e:
        res = {}
        logger.error('submit background job is failed due to {}'.format(e))
        res['success'] = False
        res['message'] = e
        return res


def create_attribute_mappings(data):
    logger.info('To Create Attribute Mappings Data')
    res = {}
    try:
        project_id = data.get("project_id")
        analysis_id = data.get("analysis_id")
        attribute_id = data.get('attribute_id', None)
        attribute_type = data.get('type')
        attribute_name = data.get('attribute_name')
        lib_type = data.get('lib_type', '')
        session_id = data.get('session_id', None)
        mappings = data.get('mappings')
        dimensions = data.get('dimensions')

        project = Projects.objects.get(id=int(project_id))

        attribute_check = AttributeMappings.objects.filter(project=project, analysis_id=analysis_id,
                                                           attribute_id=attribute_id, attribute_type=attribute_type)

        if attribute_check:
            attribute_record = AttributeMappings.objects.get(project=project, analysis_id=analysis_id,
                                                             attribute_id=attribute_id, attribute_type=attribute_type)
            attribute_record.attribute_id = attribute_id
            attribute_record.attribute_type = attribute_type
            attribute_record.attribute_name = attribute_name
            attribute_record.session_id = session_id
            attribute_record.lib_type = lib_type
            attribute_record.mappings = mappings
            attribute_record.dimensions = dimensions
            attribute_record.save()
        else:
            AttributeMappings.objects.create(project=project, analysis_id=analysis_id, attribute_id=attribute_id,
                                             attribute_type=attribute_type, attribute_name=attribute_name,
                                             session_id=session_id, lib_type=lib_type, mappings=mappings,
                                             dimensions=dimensions)
        res["success"] = True
        res["message"] = "{} attribute has been created successfully".format(attribute_type)
        return res

    except Exception as e:
        res = {}
        logger.error('Create Attribute Mappings Data is failed due to {}'.format(e))
        res['success'] = False
        res['message'] = e
        return res


def rename_output_csv_header(data, project_id, analysis_id):
    logger.info('To Rename Output CSV Header')
    res = {}
    try:
        attributes_dict = {}
        attributes_list = AttributeMappings.objects.filter(project__id=int(project_id), analysis_id=analysis_id)
        for attribute in attributes_list:
            attributes_dict[attribute.attribute_id] = attribute.attribute_name
        data.rename(columns=(attributes_dict), inplace=True)
        data.head()
        res['data'] = data
        res["success"] = True
        res["message"] = "Column name has been changed successfully"
        return res

    except Exception as e:
        res = {}
        logger.error('Rename output csv header failed due to {}'.format(e))
        res['success'] = False
        res['message'] = e
        return res
