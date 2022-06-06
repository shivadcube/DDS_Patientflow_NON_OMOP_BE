import environ
env = environ.Env()
environ.Env.read_env()

LIVY_HOST = env('LIVY_HOST')
LIVY_DATA = {"kind": "pyspark", "driverMemory":"2G", "executorMemory":"2G",  "conf":{"spark.serializer":"org.apache.spark.serializer.KryoSerializer"}}
LIVY_HEADERS = {'Content-Type': 'application/json'}


ZEPPELIN_HOST = env('ZEPPELIN_HOST')

databricks_database = env('databricks_database')

TOKEN = env('TOKEN')

WORKSPACE_URL = env('WORKSPACE_URL')

WORKSPACE_ID = env('WORKSPACE_ID')

CLUSTER_ID = env('CLUSTER_ID')

driver_path = env('driver_path')

JDBC_URL = "jdbc:spark://{}:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/{}/{};AuthMech=3;UID={};PWD={}".format(WORKSPACE_URL, WORKSPACE_ID, CLUSTER_ID, 'token', TOKEN)

S3_PROJECT_PATH = env('S3_PROJECT_PATH')

S3_BUCKET = env('S3_BUCKET')

CB_OUTPUT_BUCKET = env('CB_OUTPUT_BUCKET') #dds-cohort-builder

CB_OUTPUT_PATH = env('CB_OUTPUT_PATH') #CB_OUTPUT_DEV

ZEPPELIN_ENVIRONMENT = env('ZEPPELIN_ENVIRONMENT')

ZEPPELIN_PORT = env('ZEPPELIN_PORT')
