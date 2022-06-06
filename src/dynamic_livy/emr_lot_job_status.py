import requests
from pprint import pprint
import logging
import boto3
from datetime import datetime
import os
import sys
import argparse
import pymysql
from dotenv import load_dotenv
import threading
import pandas as pd
import json
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from mail.mail_template import MailTemplate

logger = logging.getLogger('LOT Status')


def mysql_connect(host, user, passwd, db, port):
    """
    :return connection: global mysql database connection
    """
    try:
        connection = pymysql.connect(
            host=host,
            user=user,
            passwd=passwd,
            db=db,
            port=int(port)
        )
        return connection, connection.cursor(), True
    except Exception as e:
        print(e)
        return "", "", False


def job_tracker():
    try:
        project_folder = os.path.expanduser(os.getcwd() + "/../settings")
        load_dotenv(os.path.join(project_folder, '.env'))
        host = os.getenv("DEFAULT_LOCAL_DATABASE_CONFIG_HOST", "user")
        user = os.getenv("DEFAULT_LOCAL_DATABASE_CONFIG_USER", "user")
        passwd = os.getenv("DEFAULT_LOCAL_DATABASE_CONFIG_PASS", "user")
        db = os.getenv("DEFAULT_LOCAL_DATABASE_CONFIG_NAME", "user")
        port = os.getenv("DEFAULT_LOCAL_DATABASE_CONFIG_PORT", "user")
        zeppelin_port = os.getenv("ZEPPELIN_PORT", "user")
        mysql_connector, sql_cursor, mysql_status = mysql_connect(host, user, passwd, db, port)
        if not mysql_status:
            sys.exit('mysql connection issue')
        sql_cursor.execute(
            "select notebook_id, zeppelin_host, analysis, attribute_type, id, paragraph_id from projects_backgroundjobs where status = 'READY' or status = 'RUNNING' ")
        rows = sql_cursor.fetchall()
        if not (rows is None):
            for rec in rows:
                if rec is not None:
                    url = "http://" + str(rec[1]) + ":" + str(zeppelin_port) + "/api/notebook/"
                    resp = requests.get(url + str(rec[0]) + '/paragraph/' + str(rec[5]))
                    status = resp.json()['body']['status']
                    print(status)
                    if status in ["FINISHED", "READY", "RUNNING"]:
                        update_query = "update projects_backgroundjobs set status = '{}', error_message = null, modified_at = '{}' where notebook_id = '{}' and id = {}".format(
                            status, datetime.now(), rec[0], rec[4])
                        sql_cursor.execute(update_query)
                        mysql_connector.commit()
                        print(sql_cursor.rowcount, "record(s) affected")
                    elif status in ["ERROR", "TIMEDOUT", "CANCELED"]:
                        error_msg = resp.json()['body']['results']['msg'][0]['data'].split('\n')[-2].replace("'", "").replace('"', "")
                        update_query = "update projects_backgroundjobs set status = '{}', modified_at = '{}', error_message = '{}' where notebook_id = '{}' and id = {}".format(
                            status, datetime.now(), error_msg, rec[0], rec[4])
                        sql_cursor.execute(update_query)
                        mysql_connector.commit()
                        print(sql_cursor.rowcount, "record(s) affected")

                    # send mail
                    database = os.getenv("DEFAULT_LOCAL_DATABASE_CONFIG_NAME")
                    read_query = "select B.project_id as project_id, B.notebook_id as notebook_id , B.status" \
                                 " as status, B.created_at as created_at, B.error_message as error_message," \
                                 " B.modified_at as modified_at, P.name as project_name, P.created_by as username," \
                                 " U.email as email, B.analysis as analysis_id from {}.projects_backgroundjobs B join" \
                                 " projects_projects P on P.id=B.project_id join auth_user U on U.username=P." \
                                 "created_by where B.notebook_id='{}' and B.analysis='{}' and B.id = {}".format(database, rec[0],
                                                                                                  rec[2], rec[4])
                    data = pd.read_sql_query(read_query, mysql_connector)
                    analysis_query = "SELECT analysis_data FROM {}.projects_analysis where project_id " \
                                     "= {} and s3_path LIKE '%{}'".format(database, data['project_id'][0],
                                                                          data['analysis_id'][0])
                    analysis_data = pd.read_sql_query(analysis_query, mysql_connector)
                    analysis_name = json.loads(analysis_data['analysis_data'][0]).get('name')
                    if status in ["FINISHED", "ERROR"]:
                        mail_obj = MailTemplate()
                        mail_id = data['email'][0]
                        output_data = data.to_dict('r')[0]
                        output_data['analysis_name'] = analysis_name
                        output_data['attribute_type'] = rec[3]
                        thread = threading.Thread(target=mail_obj.send_template,
                                                  kwargs={'status': status,
                                                          'to_address': [mail_id],
                                                          'data': output_data})
                        thread.start()
        sql_cursor.execute("select cluster_master_ip from projects_clusterdetails")
        rows = sql_cursor.fetchone()
        if not (rows is None):
            url = "http://" + str(rows[0]) + ":" + str(zeppelin_port) + "/api/notebook"
            notebooks = requests.get(url).json()['body']
            for note in notebooks:
                print(note['id'])
                response = requests.get(url + "/" + note["id"])
                paragraphs = response.json()['body']['paragraphs']
                if (paragraphs != []) and (paragraphs[-1]['status'] in ['FINISHED', 'ERROR']):
                    last_run = paragraphs[-1]['dateFinished'].split()
                    last_run = " ".join(last_run).replace(",", "")
                    diff = datetime.utcnow() - datetime.strptime(last_run, "%b %d %Y %I:%M:%S %p")
                    duration = diff.total_seconds()
                    if duration > 1800:
                        del_resp = requests.delete(url + "/" + str(note["id"]))
                        print(del_resp.json())
                        print(datetime.utcnow())
                    else:
                        print(duration)
                else:
                    print("notebook not yet created")

        else:
            print("LoTs status Updated Successfully")
            sql_cursor.close()
    except Exception as e:
        status = "FAILED"
        raise e


print(job_tracker())

