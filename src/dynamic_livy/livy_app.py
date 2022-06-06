import json, requests, textwrap, time
from dynamic_livy import constants as Constants
import logging

class LivySession:
    """
    Class contains all the functions related to LIVY service
    """
    def __init__(self):
        """ Initialize all class level variables """
        pass

    def spark_submit(self, session_id, code):
        try:
            data = {"code": textwrap.dedent(code)}
            statements_url = Constants.LIVY_HOST +'/sessions/' + str(session_id) + '/statements'
            submit_code = requests.post(statements_url, data=json.dumps(data), headers=Constants.LIVY_HEADERS)
            status = submit_code.json()['state']
            while status != 'available' or status != "error":
                try:
                    statement_id = submit_code.json()['id']
                    get_status = requests.get(statements_url+'/'+str(statement_id), headers=Constants.LIVY_HEADERS)
                    time.sleep(1)
                    status = get_status.json()['state']
                    if status == 'available' or status == 'idle':
                        if get_status.json()['output']['status'] != "error":
                            output =  get_status.json()['output']['data']['text/plain']
                            return(output)
                        else:
                            raise Exception(get_status.json()['output'])
                    elif status == 'waiting' or status == 'busy':
                        continue
                    elif status == 'error':
                        raise get_status.json()['evalue']
                except Exception as e:
                    raise
        except Exception as e:
            raise e

    def get_status(self, session_id):
        """
        This method is used to get status of a Livy session
        :return: Status
        """
        try:
            request = requests.get(Constants.LIVY_HOST + '/sessions/' + str(session_id), headers=Constants.LIVY_HEADERS)
            status = request.json()['state']
            while status != 'idle' or status != "error" :
                try:
                    session_url = Constants.LIVY_HOST + '/sessions/' + str(session_id)
                    r = requests.get(session_url, headers=Constants.LIVY_HEADERS) #checking status of the session
                    time.sleep(1)
                    status = r.json()['state']
                    if status == 'idle':
                        return('idle')
                    elif status == 'starting':
                        continue
                    else:
                        raise Exception("Failed to start Spark session")
                except Exception as e:
                        raise e
        except Exception as e:
                raise e

    def create_session(self):
        """
        This method is used to start a new Livy session
        :return: Livy session id
        """
        try:
            request = requests.post(Constants.LIVY_HOST + '/sessions', data=json.dumps(Constants.LIVY_DATA),
                                    headers=Constants.LIVY_HEADERS)
            return(request.json()['id'])
        except Exception as e:
                raise e

    def stop_session(self,session_id):
        try:
            session_url = Constants.LIVY_HOST + '/sessions/' + str(session_id)
            response = requests.delete(session_url, headers=Constants.LIVY_HEADERS)
            if  response.json()['msg'] == 'deleted':
                return "Success"
            else:
                raise Exception(response.json())
        except Exception as e:
            raise e
