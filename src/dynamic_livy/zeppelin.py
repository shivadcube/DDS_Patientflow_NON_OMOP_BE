import requests
import json
from . import constants as const


class ZeppelinInterpreter:
    """
    Class contains all the functions related to Zeppelin services
    """

    def __init__(self):
        """ Initialize all class level variables """
        pass

    def create_notebook(self, master_ip, notebook_name):
        try:
            headers = {'Content-type': 'application/json'}
            data = {"name": '/PFA_Notebooks/' + notebook_name}
            notebook_api = "http://" + str(master_ip) + ":" + str(const.ZEPPELIN_PORT) + '/api/notebook'
            r = requests.post(notebook_api, data=json.dumps(data), headers=headers)
            if r.status_code != 200:
                raise Exception('Failed to create zeppelin notebook for - "{}", Message:"{}"'.format(notebook_name,
                                                                                                     r.json()[
                                                                                                         "message"]))
            notebook_id = r.json()['body']
            return notebook_id
        except Exception as e:
            raise e

    def check_notebook_status(self, master_ip, notebook_id):
        try:
            notebook_api = "http://" + str(master_ip) + ":" + str(const.ZEPPELIN_PORT) + '/api/notebook/' + notebook_id
            r = requests.get(notebook_api)
            if r.status_code != 200:
                return False
            return True
        except Exception as e:
            raise e

    def append_paragraph(self, master_ip, notebook_id, spark_code):
        try:
            headers = {'Content-type': 'application/json'}
            data = {
                "title": "pyspark",
                "text": "%spark.pyspark\n\n" + spark_code,
            }
            paragraph_api = "http://" + str(master_ip) + ":" + str(
                const.ZEPPELIN_PORT) + '/api/notebook/{}/paragraph'.format(notebook_id)
            r = requests.post(paragraph_api, data=json.dumps(data), headers=headers)
            if r.status_code not in [201, 200]:
                raise Exception("{}".format(r.json()["message"]))
            return r.json()['body']
        except Exception as e:
            raise e

    def spark_submit(self, master_ip, notebook_id, spark_code):
        try:
            paragraph_id = self.append_paragraph(master_ip, notebook_id, spark_code)
            run_paragraph_api = "http://" + str(master_ip) + ":" + str(
                const.ZEPPELIN_PORT) + '/api/notebook/run/{}/{}'.format(notebook_id, paragraph_id)
            r = requests.post(run_paragraph_api)
            if r.status_code not in [201, 200]:
                raise Exception('Failed to run Paragraph')
            elif r.json()['body']['code'] in ['ERROR']:
                error_msg = r.json()['body']['msg'][0]['data'].split('\n', 1)[0]
                if 'InterpreterException' in error_msg or 'RuntimeException' in error_msg:
                    error_msg = "Cluster auto scaling has been initiated. Click on the home page and try after few minutes"
                    self.delete_notebook(master_ip, notebook_id)
                elif 'Python process is abnormally exited' in error_msg:
                    error_msg = "Application container memory has been exceeded, Please reload the project once again."
                    self.delete_notebook(master_ip, notebook_id)
                raise Exception(error_msg)
            elif r.json()['body']['code'] in ['ABORT', 'CANCEL']:
                raise Exception("Something went wrong in the zeppelin")
            elif not r.json()['body']['msg']:
                return paragraph_id
            else:
                return r.json()['body']['msg'][0]['data']
        except Exception as e:
            raise e

    def delete_notebook(self, master_ip, notebook_id):
        try:
            notebook_api = "http://" + str(master_ip) + ":" + str(const.ZEPPELIN_PORT) + '/api/notebook/' + notebook_id
            r = requests.delete(notebook_api)
            if r.status_code != 200:
                return False
            return True
        except Exception as e:
            raise e

    def spark_job_submit(self, master_ip, notebook_id, paragraph_id):
        try:
            run_paragraph_api = "http://" + str(master_ip) + ":" + str(
                const.ZEPPELIN_PORT) + '/api/notebook/run/{}/{}'.format(notebook_id, paragraph_id)
            r = requests.post(run_paragraph_api)
            if r.status_code not in [201, 200]:
                raise Exception('Failed to run Paragraph')
            elif r.json()['body']['code'] in ['ERROR']:
                error_msg = r.json()['body']['msg'][0]['data'].split('\n', 1)[0]
                if 'InterpreterException' in error_msg or 'RuntimeException' in error_msg:
                    error_msg = "Cluster auto scaling has been initiated. Click on the home page and try after few minutes"
                    self.delete_notebook(master_ip, notebook_id)
                elif 'Python process is abnormally exited' in error_msg:
                    error_msg = "Application container memory has been exceeded, Please reload the project once again."
                    self.delete_notebook(master_ip, notebook_id)
                raise Exception(error_msg)
            elif r.json()['body']['code'] in ['ABORT', 'CANCEL']:
                raise Exception("Something went wrong in the zeppelin")
            elif not r.json()['body']['msg']:
                return paragraph_id
            else:
                return r.json()['body']['msg'][0]['data']
        except Exception as e:
            raise e
