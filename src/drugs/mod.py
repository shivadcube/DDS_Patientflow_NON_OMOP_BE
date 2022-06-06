import json
import logging
import pandas as pd
import requests
from datetime import datetime
from py2neo import Graph
from py2neo import Node

from drugs import constants

logger = logging.getLogger('generic.logger')


def connect_to_neo4j():
    try:
        graph = Graph(constants.NEO4J_HOST, auth=(constants.NEO4J_USER,
                                           constants.NEO4J_PASSWORD))

        return graph

    except Exception as e:
        raise Exception(e)


def ndc_drug_data(codes_list):
    logger.info('fetching ndc code data of codes {}'.format(codes_list))
    res = {}
    graph = connect_to_neo4j()
    try:
        matched_data = graph.run(constants.MATCHED_DRUG_QUERY.format(codes_list)).data()
        matched_codes = [data['actual_ndc'] for data in graph.run(constants.MATCHED_DRUG_QUERY.format(codes_list)).data()]
        unmatched_data = []
        for data in codes_list:
            if data not in matched_codes:
                unmatched_data.append(data)
        res['matched_codes'] = matched_data
        res['unmatched_codes'] = unmatched_data
        res['mapping_list'] = {}
        res['success'] = True
        return res

    except Exception as e:
        res['success'] = False
        res['message'] = e
        return res
