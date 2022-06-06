import ast
import logging

from dynamic_livy.zeppelin import ZeppelinInterpreter
from dynamic_livy import pandas_lib
from dynamic_livy import pyspark_lib
from projects.models import ClusterDetails
from services.livy.constants import CLUSTER_NAME

logger = logging.getLogger('generic.logger')

zp = ZeppelinInterpreter()

pandas_pf = pandas_lib.PatientFlow()
pyspark_pf = pyspark_lib.PatientFlow()
pandas_lot = pandas_lib.LOT()
pyspark_lot = pyspark_lib.LOT()


def attributes_mapping(request, data):
    logger.info('mapping attributes of lot ')
    res = {}
    try:
        cluster = ClusterDetails.objects.get(cluster_name=CLUSTER_NAME)
        if data.get('lib_type', '') == 'pyspark':
            lot = pyspark_lot
        else:
            lot = pandas_lot

        lot_mappings_json = data.get('lot_json')
        attribute_id = data.get('attribute_id')
        session_id = data.get('session_id')
        analysis_id = data.get("analysis_id")
        start_date = data.get('date_ranges')['analysis_period'].get('start_date')
        end_date = data.get('date_ranges')['analysis_period'].get('end_date')

        result = lot.create_regimen(analysis_id,
                                    lot_mappings_json,
                                    attribute_id, start_date, end_date)
        zp.spark_submit(cluster.cluster_master_ip, session_id, result)

        res["success"] = True
        return res

    except Exception as e:
        logger.error('mapping attributes of lot'
                     ' is failed due to {}'.format(e))
        res['success'] = False
        res['message'] = e
        return res


def deep_dive_analysis(request, data):
    logger.info('deep dive analysis of lot ')
    res = {}
    try:
        cluster = ClusterDetails.objects.get(cluster_name=CLUSTER_NAME)
        if data.get('lib_type', '') == 'pyspark':
            lot = pyspark_lot
        else:
            lot = pandas_lot

        analysis_id = data.get('analysis_id')
        attribute_id = data.get('attribute_id')
        session_id = data.get('session_id')
        lot_attributes = data.get('lot_attributes')
        start_date = data.get('date_ranges')['analysis_period'].get('start_date')
        end_date = data.get('date_ranges')['analysis_period'].get('end_date')
        line_number = data.get('line_number')
        regimen = data.get('regimen')

        result = lot.deep_dive_lot(analysis_id,
                                   'lot_' + str(attribute_id),
                                   start_date,
                                   end_date,
                                   lot_attributes
                                   )
        right_side_regimen = lot.deep_dive_summary(analysis_id,
                                                   'lot_' + str(attribute_id),
                                                   line_number,
                                                   regimen,
                                                   start_date,
                                                   end_date,
                                                   lot_attributes)

        submit = zp.spark_submit(cluster.cluster_master_ip, session_id, result)
        right_graph = zp.spark_submit(cluster.cluster_master_ip, session_id, right_side_regimen)
        res["success"] = True
        res['data'] = ast.literal_eval(submit)
        res['right_graph'] = ast.literal_eval(right_graph)
        return res

    except Exception as e:
        res = {}
        logger.error('deep dive analysis of lot'
                     ' is failed due to {}'.format(e))
        res['success'] = False
        res['message'] = e
        return res


def view_regimen(request, data):
    logger.info('regimen view of {}'.format(data))
    res = {}
    try:
        cluster = ClusterDetails.objects.get(cluster_name=CLUSTER_NAME)
        if data.get('lib_type', '') == 'pyspark':
            lot = pyspark_lot
        else:
            lot = pandas_lot
        analysis_id = data.get('analysis_id')
        attribute_id = data.get('attribute_id')
        session_id = data.get('session_id')
        lot_attributes = data.get('lot_attributes')
        regimen = data.get('regimen')
        line_number = data.get('line_number')
        start_date = data.get('date_ranges')['analysis_period'] \
            .get('start_date')
        end_date = data.get('date_ranges')['analysis_period'].get('end_date')
        left_side_regimen = lot.deep_dive_regimen(analysis_id,
                                                  'lot_' + str(attribute_id),
                                                  line_number,
                                                  regimen,
                                                  start_date,
                                                  end_date,
                                                  lot_attributes)
        left_graph = zp.spark_submit(cluster.cluster_master_ip, session_id, left_side_regimen)

        right_side_regimen = lot.deep_dive_summary(analysis_id,
                                                   'lot_' + str(attribute_id),
                                                   line_number,
                                                   regimen,
                                                   start_date,
                                                   end_date,
                                                   lot_attributes)

        right_graph = zp.spark_submit(cluster.cluster_master_ip, session_id, right_side_regimen)

        res["success"] = True
        res['left_graph'] = ast.literal_eval(left_graph)
        res['right_graph'] = ast.literal_eval(right_graph)
        return res

    except Exception as e:
        res = {}
        logger.error('regimen view  of lot'
                     ' is failed due to {}'.format(e))
        res['success'] = False
        res['message'] = e
        return res


def deep_dive_acceptance_analysis(request, data):
    logger.info('deep dive analysis of lot ')
    res = {}
    try:
        analysis_id = data.get('analysis_id')
        attribute_id = data.get('attribute_id')
        session_id = data.get('session_id')
        lot_attributes = data.get('lot_attributes')
        start_date = data.get('date_ranges')['analysis_period'].get('start_date')
        end_date = data.get('date_ranges')['analysis_period'].get('end_date')
        line_number = data.get('line_number', 1)
        regimen = data.get('regimen')

        cluster = ClusterDetails.objects.get(cluster_name=CLUSTER_NAME)
        if data.get('lib_type', '') == 'pyspark':
            lot = pyspark_lot
        else:
            lot = pandas_lot

        result = lot.patient_share(analysis_id,
                                   'lot_' + str(attribute_id),
                                   start_date,
                                   end_date, lot_attributes
                                   )
        submit = zp.spark_submit(cluster.cluster_master_ip, session_id, result)
        graph_data = graph_ab_json_conversion(ast.literal_eval(submit))

        result_c = lot.deep_dive_lot_regimen(analysis_id,
                                             'lot_' + str(attribute_id),
                                             start_date,
                                             end_date, None, None, lot_attributes
                                             )
        submit_c = zp.spark_submit(cluster.cluster_master_ip, session_id, result_c)
        graph_data_c = ast.literal_eval(submit_c)


        regimen = list(graph_data_c['Stack'][1].keys())[0]
        if regimen == 'Others':
            regimen = list(graph_data_c['Stack'][1].keys())[1]
        result_d = lot.deep_dive_lot_regimen(analysis_id,
                                             'lot_' + str(attribute_id),
                                             start_date,
                                             end_date, line_number, regimen, lot_attributes
                                             )
        submit_d = zp.spark_submit(cluster.cluster_master_ip, session_id, result_d)
        graph_data_d = ast.literal_eval(submit_d)

        result_e = lot.regimen_combomono_share(analysis_id,
                                               'lot_' + str(attribute_id),
                                               start_date,
                                               end_date, lot_attributes
                                               )
        submit_e = zp.spark_submit(cluster.cluster_master_ip, session_id, result_e)
        graph_data_e = graph_e_json_conversion(ast.literal_eval(submit_e))
        result_f = lot.regimen_drugs_share(analysis_id,
                                           'lot_' + str(attribute_id),
                                           start_date,
                                           end_date,
                                           line_number,
                                           lot_attributes,
                                           graph_data_e[1]
                                           )
        submit_f = zp.spark_submit(cluster.cluster_master_ip, session_id, result_f)
        graph_data_f = graph_f_json_conversion(ast.literal_eval(submit_f))

        result_gh = lot.average_treatment_days(analysis_id,
                                               'lot_' + str(attribute_id),
                                               start_date,
                                               end_date, lot_attributes)
        submit_gh = zp.spark_submit(cluster.cluster_master_ip, session_id, result_gh)
        graph_data_gh = ast.literal_eval(submit_gh)

        graph_data["StackBarA"] = float_to_string_conversion(graph_data_c['Stack'])
        graph_data["StackBarB"] = float_to_string_conversion(graph_data_d['Stack'])
        graph_data['MonoComboA'] = graph_data_e[0]
        graph_data['MonoComboB'] = graph_data_f
        graph_data['AverageDays'] = float_to_string_conversion(graph_data_gh)
        graph_data['dependant_keys'] = {'2B_line': 1, '2B_regimen': regimen, '3B_line': 1, '3B_regimen': graph_data_e[1].capitalize()}
        graph_data['distinct_regimen'] = distinct_regimen(graph_data_c['Stack'], graph_data_d['Stack'], graph_data_f, graph_data_gh)
        res["success"] = True
        res['data'] = graph_data
        return res

    except Exception as e:
        res = {'success': False, 'message': e}
        return res


def deep_dive_analysis_stack_bar(request, data):
    logger.info('deep dive analysis of lot ')
    res = {}
    try:
        analysis_id = data.get('analysis_id')
        attribute_id = data.get('attribute_id')
        session_id = data.get('session_id')
        lot_attributes = data.get('lot_attributes')
        start_date = data.get('date_ranges')['analysis_period'].get('start_date')
        end_date = data.get('date_ranges')['analysis_period'].get('end_date')
        line_number = data.get('line_number')
        regimen = data.get('regimen')

        cluster = ClusterDetails.objects.get(cluster_name=CLUSTER_NAME)
        if data.get('lib_type', '') == 'pyspark':
            lot = pyspark_lot
        else:
            lot = pandas_lot

        result = lot.deep_dive_lot_regimen(analysis_id,
                                           'lot_' + str(attribute_id),
                                           start_date,
                                           end_date, line_number, regimen, lot_attributes
                                           )
        submit = zp.spark_submit(cluster.cluster_master_ip, session_id, result)
        graph_data = ast.literal_eval(submit)
        res["success"] = True
        res['data'] = float_to_string_conversion(graph_data['Stack'])
        return res

    except Exception as e:
        res = {'success': False, 'message': e}
        return res


def deep_dive_analysis_mono_combo(request, data):
    logger.info('deep dive analysis of lot ')
    res = {}
    try:
        analysis_id = data.get('analysis_id')
        attribute_id = data.get('attribute_id')
        session_id = data.get('session_id')
        lot_attributes = data.get('lot_attributes')
        start_date = data.get('date_ranges')['analysis_period'].get('start_date')
        end_date = data.get('date_ranges')['analysis_period'].get('end_date')
        line_number = data.get('line_number')
        regimen = data.get('regimen')

        cluster = ClusterDetails.objects.get(cluster_name=CLUSTER_NAME)
        if data.get('lib_type', '') == 'pyspark':
            lot = pyspark_lot
        else:
            lot = pandas_lot

        result = lot.regimen_drugs_share(analysis_id,
                                         'lot_' + str(attribute_id),
                                         start_date,
                                         end_date, line_number, lot_attributes, regimen.lower()
                                         )
        submit = zp.spark_submit(cluster.cluster_master_ip, session_id, result)
        graph_data = graph_f_json_conversion(ast.literal_eval(submit))
        res["success"] = True
        res['data'] = graph_data
        return res

    except Exception as e:
        res = {'success': False, 'message': e}
        return res


def deep_dive_analysis_average_days(request, data):
    logger.info('deep dive analysis of lot ')
    res = {}
    try:
        analysis_id = data.get('analysis_id')
        attribute_id = data.get('attribute_id')
        session_id = data.get('session_id')
        lot_attributes = data.get('lot_attributes')
        start_date = data.get('date_ranges')['analysis_period'].get('start_date')
        end_date = data.get('date_ranges')['analysis_period'].get('end_date')

        cluster = ClusterDetails.objects.get(cluster_name=CLUSTER_NAME)
        if data.get('lib_type', '') == 'pyspark':
            lot = pyspark_lot
        else:
            lot = pandas_lot

        result = lot.average_treatment_days(analysis_id,
                                            'lot_' + str(attribute_id),
                                            start_date,
                                            end_date, lot_attributes
                                            )
        submit = zp.spark_submit(cluster.cluster_master_ip, session_id, result)
        graph_data = ast.literal_eval(submit)
        res["success"] = True
        res['data'] = graph_data
        return res

    except Exception as e:
        res = {'success': False, 'message': e}
        return res


def graph_ab_json_conversion(data):
    output_dict = {}
    funnel_list = []
    table_list = []
    for k, v in data['graph'].items():
        if str(int(k)) == '5':
            k = '5+'
        else:
            k = str(int(k))
        funnel_list.append({"line_name": "Line {}".format(k), "patients": list(v.keys())[0],
                            "perc": "{}%".format(round(float(list(v.values())[0]), 1))})
    for i in data['table']:
        if str(int(i.get('Line'))) == '5':
            line = '5+'
        else:
            line = str(int(i.get('Line')))
        table_list.append({'line_name': 'Line {}'.format(line), 'total_patients': i.get('Total'),
                           'continued': i.get('Continued').get('Person_count'),
                           'continued_perc': "{}%".format(round(i.get('Continued').get('Percentage'), 1)),
                           'new': i.get('New').get('Person_count'),
                           'new_perc': "{}%".format(round(i.get('New').get('Percentage'), 1)),
                           'progressed': i.get('Progressed').get('Person_count'),
                           'progressed_perc': "{}%".format(round(i.get('Progressed').get('Percentage'), 1)),
                           'dropped_off': i.get('DroppedOff').get('Person_count'),
                           'dropped_off_perc': "{}%".format(round(i.get('DroppedOff').get('Percentage'), 1))})
    table_length = len(data['table'])
    if table_length < 5:
        for i in range(table_length + 1, 6):
            k = i
            if i == 5:
                k = '5+'
            table_list.append({'line_name': 'Line {}'.format(k), 'total_patients': 0,
                               'continued': 0,
                               'continued_perc': "0%",
                               'new': 0,
                               'new_perc': "0%",
                               'progressed': 0,
                               'progressed_perc': "0%",
                               'dropped_off': 0,
                               'dropped_off_perc': "0%"})
    funnel_length = len(data['graph'])
    if funnel_length < 5:
        for j in range(funnel_length + 1, 6):
            k = j
            if j == 5:
                k = "5+"
            funnel_list.append({"line_name": "Line {}".format(k), "patients": 0,
                                "perc": "0%"})

    output_dict["Patient_Funnel_Data"] = funnel_list
    output_dict["Patient_Table_Data"] = table_list
    return output_dict


def graph_e_json_conversion(data):
    output_dict = {}
    mono_combo_a = []
    res = 'mono'
    for k, v in data.items():
        if str(int(k)) == '5':
            k = '5+'
        else:
            k = str(int(k))
        if v.get('mono') and v.get('combo'):
            if str(k) == '1':
                res_dict = {}
                res_list = list(v.items())
                for i in res_list:
                    res_dict[i[0]] = int(i[1].split(', ')[0])
                res = list({key: value for key, value in sorted(res_dict.items(), key=lambda item: item[1])})[1]
            mono_combo_a.append(
                {"Name": "Line {}".format(k), "Mono": v['mono'].split(', ')[0], "Combo": v['combo'].split(', ')[0],
                 "Mono_Perc": "{}".format(round(float(v['mono'].split(', ')[1]), 1)),
                 "Combo_Perc": "{}".format(round(float(v['combo'].split(', ')[1]), 1))})
        elif v.get('mono'):
            if str(k) == '1':
                res = list(v.keys())[0]
            mono_combo_a.append({"Name": "Line {}".format(k), "Mono": v['mono'].split(', ')[0],
                                 "Combo": "0", "Combo_Perc": "0",
                                 "Mono_Perc": "{}".format(round(float(v['mono'].split(', ')[1]), 1))})
        else:
            if str(k) == '1':
                res = list(v.keys())[0]
            mono_combo_a.append({"Name": "Line {}".format(k), "Combo": v['combo'].split(', ')[0],
                                 "Mono": "0", "Mono_Perc": "0",
                                 "Combo_Perc": "{}".format(round(float(v['combo'].split(', ')[1]), 1))})

    output_dict["MonoComboA"] = mono_combo_a
    return mono_combo_a, res


def graph_f_json_conversion(data):
    output_dict = {}
    mono_combo_b = []
    for k, v in data.items():
        mono_combo_b.append({"name": k, "patients": list(v.keys())[0], "perc": round(list(v.values())[0], 1)})
    output_dict["MonoComboB"] = mono_combo_b
    return mono_combo_b


def distinct_regimen(stack_data_a, stack_data_b, mono_combo_data, average_data):
    distinct_reg = []
    for k, v in stack_data_a.items():
        distinct_reg.extend(list(v.keys()))
    for k, v in stack_data_b.items():
        distinct_reg.extend(list(v.keys()))
    for k, v in average_data.items():
        distinct_reg.extend(list(v.keys()))
    for i in mono_combo_data:
        distinct_reg.append(i.get('name'))
    return list(set(distinct_reg))


def float_to_string_conversion(data):
    stack_dict = {}
    for k, v in data.items():
        if int(k) == 5:
            stack_dict["5+"] = v
        else:
            stack_dict[str(int(k))] = v
    return stack_dict
