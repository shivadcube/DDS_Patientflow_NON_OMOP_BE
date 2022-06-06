import environ
env = environ.Env()
environ.Env.read_env()

CLUSTER_NAME = env('CLUSTER_NAME')

EMR_REGION = env('EMR_REGION')

CB_OUTPUT_BUCKET = env('CB_OUTPUT_BUCKET')

LogUri = env('LogUri')
ReleaseLabel = env('ReleaseLabel')
Ec2KeyName = env('Ec2KeyName')
EmrManagedSlaveSecurityGroup = env('EmrManagedSlaveSecurityGroup')
EmrManagedMasterSecurityGroup = env('EmrManagedMasterSecurityGroup')
ServiceAccessSecurityGroup = env('ServiceAccessSecurityGroup')
Ec2SubnetId = env('Ec2SubnetId')
ScriptBootstrapAction = env('ScriptBootstrapAction')
PatchBootstrapAction = env('PatchBootstrapAction')
JobFlowRole = env('JobFlowRole')
ServiceRole = env('ServiceRole')
CORE_INSTANCE_TYPE = env('CORE_INSTANCE_TYPE')
CORE_INSTANCE_COUNT = env('CORE_INSTANCE_COUNT')
TASK_INSTANCE_TYPE = env('TASK_INSTANCE_TYPE')
TASK_INSTANCE_COUNT = env('TASK_INSTANCE_COUNT')
MASTER_INSTANCE_TYPE = env('MASTER_INSTANCE_TYPE')
MASTER_INSTANCE_COUNT = env('MASTER_INSTANCE_COUNT')

ZEPPELIN_DRIVER_MEMORY = env('ZEPPELIN_DRIVER_MEMORY')

ZEPPELIN_DRIVER_CORES = env('ZEPPELIN_DRIVER_CORES')

DRIVER_MAX_RESULT_SIZE = env('DRIVER_MAX_RESULT_SIZE')

ZEPPELIN_EXECUTOR_MEMORY = env('ZEPPELIN_EXECUTOR_MEMORY')

ZEPPELIN_EXECUTOR_INSTANCES = env('ZEPPELIN_EXECUTOR_INSTANCES')

ZEPPELIN_EXECUTOR_CORES = env('ZEPPELIN_EXECUTOR_CORES')

SPARK_DYNAMIC_ALLOCATION = env('SPARK_DYNAMIC_ALLOCATION')

DYNAMIC_ALLOCATION_MIN_EXECUTOR = env('DYNAMIC_ALLOCATION_MIN_EXECUTOR')

DYNAMIC_ALLOCATION_MAX_EXECUTOR = env('DYNAMIC_ALLOCATION_MAX_EXECUTOR')

ZEPPELIN_PORT = env('ZEPPELIN_PORT')

UAT_SPARK_INTERPRETER = {
  "name": "spark",
  "group": "spark",
  "properties": {
    "spark.submit.deployMode": {
      "name": "spark.submit.deployMode",
      "value": "cluster",
      "type": "string",
      "description": "The deploy mode of Spark driver program, either ' client ' or 'cluster ', Which means to launch driver program locally (' client ') or remotely (' cluster ') on one of the nodes inside the cluster."
    },
    "spark.app.name": {
      "name": "spark.app.name",
      "value": "Zeppelin",
      "type": "string",
      "description": "The name of spark application."
    },
    "zeppelin.spark.useHiveContext": {
      "name": "zeppelin.spark.useHiveContext",
      "value": "true",
      "type": "string",
      "description": "Use HiveContext instead of SQLContext if it is true. Enable hive for SparkSession."
    },
    "zeppelin.spark.printREPLOutput": {
      "name": "zeppelin.spark.printREPLOutput",
      "value": "true",
      "type": "string",
      "description": "Print REPL output"
    },
    "zeppelin.spark.maxResult": {
      "name": "zeppelin.spark.maxResult",
      "value": "1000",
      "type": "string",
      "description": "Max number of result to display."
    },
    "zeppelin.spark.concurrentSQL": {
      "name": "zeppelin.spark.concurrentSQL",
      "value": "false",
      "type": "string",
      "description": "Execute multiple SQL concurrently if set true."
    },
    "spark.yarn.jar": {
      "name": "spark.yarn.jar",
      "value": "",
      "type": "string"
    },
    "master": {
      "name": "master",
      "value": "yarn",
      "type": "string"
    },
    "args": {
      "name": "args",
      "value": "",
      "type": "string"
    },
    "spark.home": {
      "name": "spark.home",
      "value": "/usr/lib/spark",
      "type": "string"
    },
    "zeppelin.spark.importImplicit": {
      "name": "zeppelin.spark.importImplicit",
      "value": "true",
      "type": "string"
    },
    "zeppelin.pyspark.python": {
      "name": "zeppelin.pyspark.python",
      "value": "python",
      "type": "string"
    },
    "zeppelin.dep.localrepo": {
      "name": "zeppelin.dep.localrepo",
      "value": "/usr/lib/zeppelin/local-repo",
      "type": "string"
    }
  },
  "interpreterGroup": [
    {
      "name": "spark",
      "class": "org.apache.zeppelin.spark.SparkInterpreter",
      "defaultInterpreter": True,
      "editor": {
        "language": "scala",
        "editOnDblClick": False,
        "completionKey": "TAB",
        "completionSupport": True
      }
    },
    {
      "name": "sql",
      "class": "org.apache.zeppelin.spark.SparkSqlInterpreter",
      "defaultInterpreter": False,
      "editor": {
        "language": "sql",
        "editOnDblClick": False,
        "completionKey": "TAB",
        "completionSupport": True
      }
    },
    {
      "name": "pyspark",
      "class": "org.apache.zeppelin.spark.PySparkInterpreter",
      "defaultInterpreter": False,
      "editor": {
        "language": "python",
        "editOnDblClick": False,
        "completionKey": "TAB",
        "completionSupport": True
      }
    },
    {
      "name": "ipyspark",
      "class": "org.apache.zeppelin.spark.IPySparkInterpreter",
      "defaultInterpreter": False,
      "editor": {
        "language": "python",
        "editOnDblClick": False,
        "completionSupport": True,
        "completionKey": "TAB"
      }
    },
    {
      "name": "r",
      "class": "org.apache.zeppelin.spark.SparkRInterpreter",
      "defaultInterpreter": False,
      "editor": {
        "language": "r",
        "editOnDblClick": False,
        "completionSupport": False,
        "completionKey": "TAB"
      }
    },
    {
      "name": "ir",
      "class": "org.apache.zeppelin.spark.SparkIRInterpreter",
      "defaultInterpreter": False,
      "editor": {
        "language": "r",
        "editOnDblClick": False,
        "completionSupport": True,
        "completionKey": "TAB"
      }
    },
    {
      "name": "shiny",
      "class": "org.apache.zeppelin.spark.SparkShinyInterpreter",
      "defaultInterpreter": False,
      "editor": {
        "language": "r",
        "editOnDblClick": False,
        "completionSupport": True,
        "completionKey": "TAB"
      }
    },
    {
      "name": "kotlin",
      "class": "org.apache.zeppelin.spark.KotlinSparkInterpreter",
      "defaultInterpreter": False,
      "editor": {
        "language": "kotlin",
        "editOnDblClick": False,
        "completionKey": "TAB",
        "completionSupport": False
      }
    }
  ],
  "dependencies": [],
  "option": {
    "remote": True,
    "port": -1,
    "perNote": "{}".format(env('PER_NOTE_STATUS')),
    "perUser": "",
    "isExistingProcess": False,
    "setPermission": False,
    "owners": [
    ],
    "isUserImpersonate": False
  }
}

PROD_SPARK_INTERPRETER = {
  "name": "spark",
  "group": "spark",
  "properties": {
    "spark.submit.deployMode": {
      "name": "spark.submit.deployMode",
      "value": "cluster",
      "type": "string",
      "description": "The deploy mode of Spark driver program, either ' client ' or 'cluster ', Which means to launch driver program locally (' client ') or remotely (' cluster ') on one of the nodes inside the cluster."
    },
    "spark.app.name": {
      "name": "spark.app.name",
      "value": "Zeppelin",
      "type": "string",
      "description": "The name of spark application."
    },
    "zeppelin.spark.useHiveContext": {
      "name": "zeppelin.spark.useHiveContext",
      "value": "true",
      "type": "string",
      "description": "Use HiveContext instead of SQLContext if it is true. Enable hive for SparkSession."
    },
    "zeppelin.spark.printREPLOutput": {
      "name": "zeppelin.spark.printREPLOutput",
      "value": "true",
      "type": "string",
      "description": "Print REPL output"
    },
    "zeppelin.spark.maxResult": {
      "name": "zeppelin.spark.maxResult",
      "value": "1000",
      "type": "string",
      "description": "Max number of result to display."
    },
    "zeppelin.spark.concurrentSQL": {
      "name": "zeppelin.spark.concurrentSQL",
      "value": "false",
      "type": "string",
      "description": "Execute multiple SQL concurrently if set true."
    },
    "spark.yarn.jar": {
      "name": "spark.yarn.jar",
      "value": "",
      "type": "string"
    },
    "master": {
      "name": "master",
      "value": "yarn",
      "type": "string"
    },
    "args": {
      "name": "args",
      "value": "",
      "type": "string"
    },
    "spark.home": {
      "name": "spark.home",
      "value": "/usr/lib/spark",
      "type": "string"
    },
    "zeppelin.spark.importImplicit": {
      "name": "zeppelin.spark.importImplicit",
      "value": "true",
      "type": "string"
    },
    "zeppelin.pyspark.python": {
      "name": "zeppelin.pyspark.python",
      "value": "python",
      "type": "string"
    },
    "zeppelin.dep.localrepo": {
      "name": "zeppelin.dep.localrepo",
      "value": "/usr/lib/zeppelin/local-repo",
      "type": "string"
    },
    "spark.driver.memory": {
      "name": "spark.driver.memory",
      "value": "{}".format(ZEPPELIN_DRIVER_MEMORY),
      "type": "string"
    },
    "spark.driver.maxResultSize": {
      "name": "spark.driver.maxResultSize",
      "value": "{}".format(DRIVER_MAX_RESULT_SIZE),
      "type": "string"
    },
    "spark.dynamicAllocation.minExecutors": {
      "name": "spark.dynamicAllocation.minExecutors",
      "value": "{}".format(DYNAMIC_ALLOCATION_MIN_EXECUTOR),
      "type": "string"
    },
    "spark.dynamicAllocation.maxExecutors": {
      "name": "spark.dynamicAllocation.maxExecutors",
      "value": "{}".format(DYNAMIC_ALLOCATION_MAX_EXECUTOR),
      "type": "string"
    },
    "spark.executor.memory": {
      "name": "spark.executor.memory",
      "value": "{}".format(ZEPPELIN_EXECUTOR_MEMORY),
      "type": "string"
    },
    "spark.executor.instances": {
      "name": "spark.executor.instances",
      "value": "{}".format(ZEPPELIN_EXECUTOR_INSTANCES),
      "type": "string"
    },
    "spark.dynamicAllocation.enabled": {
      "name": "spark.dynamicAllocation.enabled",
      "value": "{}".format(SPARK_DYNAMIC_ALLOCATION),
      "type": "string"
    },
    "spark.sql.extensions": {
      "name": "spark.sql.extensions",
      "value": "io.delta.sql.DeltaSparkSessionExtension",
      "type": "string"
    },
    "spark.delta.logStore.class": {
      "name": "spark.delta.logStore.class",
      "value": "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
      "type": "string"
    },
    "spark.sql.catalog.spark_catalog": {
      "name": "spark.sql.catalog.spark_catalog",
      "value": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
      "type": "string"
    },
    "spark.databricks.delta.checkLatestSchemaOnRead": {
      "name": "spark.databricks.delta.checkLatestSchemaOnRead",
      "value": "false",
      "type": "string"
    },
    "spark.sql.shuffle.partitions": {
      "name": "spark.sql.shuffle.partitions",
      "value": "100",
      "type": "string"
    },
    "spark.sql.adaptive.coalescePartitions.enabled": {
      "name": "spark.sql.adaptive.coalescePartitions.enabled",
      "value": "true",
      "type": "string"
    },
    "spark.sql.adaptive.coalescePartitions.minPartitionNum": {
      "name": "spark.sql.adaptive.coalescePartitions.minPartitionNum",
      "value": "10",
      "type": "string"
    }
  },
  "interpreterGroup": [
    {
      "name": "spark",
      "class": "org.apache.zeppelin.spark.SparkInterpreter",
      "defaultInterpreter": True,
      "editor": {
        "language": "scala",
        "editOnDblClick": False,
        "completionKey": "TAB",
        "completionSupport": True
      }
    },
    {
      "name": "sql",
      "class": "org.apache.zeppelin.spark.SparkSqlInterpreter",
      "defaultInterpreter": False,
      "editor": {
        "language": "sql",
        "editOnDblClick": False,
        "completionKey": "TAB",
        "completionSupport": True
      }
    },
    {
      "name": "pyspark",
      "class": "org.apache.zeppelin.spark.PySparkInterpreter",
      "defaultInterpreter": False,
      "editor": {
        "language": "python",
        "editOnDblClick": False,
        "completionKey": "TAB",
        "completionSupport": True
      }
    },
    {
      "name": "ipyspark",
      "class": "org.apache.zeppelin.spark.IPySparkInterpreter",
      "defaultInterpreter": False,
      "editor": {
        "language": "python",
        "editOnDblClick": False,
        "completionSupport": True,
        "completionKey": "TAB"
      }
    },
    {
      "name": "r",
      "class": "org.apache.zeppelin.spark.SparkRInterpreter",
      "defaultInterpreter": False,
      "editor": {
        "language": "r",
        "editOnDblClick": False,
        "completionSupport": False,
        "completionKey": "TAB"
      }
    },
    {
      "name": "ir",
      "class": "org.apache.zeppelin.spark.SparkIRInterpreter",
      "defaultInterpreter": False,
      "editor": {
        "language": "r",
        "editOnDblClick": False,
        "completionSupport": True,
        "completionKey": "TAB"
      }
    },
    {
      "name": "shiny",
      "class": "org.apache.zeppelin.spark.SparkShinyInterpreter",
      "defaultInterpreter": False,
      "editor": {
        "language": "r",
        "editOnDblClick": False,
        "completionSupport": True,
        "completionKey": "TAB"
      }
    },
    {
      "name": "kotlin",
      "class": "org.apache.zeppelin.spark.KotlinSparkInterpreter",
      "defaultInterpreter": False,
      "editor": {
        "language": "kotlin",
        "editOnDblClick": False,
        "completionKey": "TAB",
        "completionSupport": False
      }
    }
  ],
  "dependencies": [{
            "groupArtifactVersion": "io.delta:delta-core_2.12:1.0.1",
            "local": False
        }],
  "option": {
    "remote": True,
    "port": -1,
    "perNote": "{}".format(env('PER_NOTE_STATUS')),
    "perUser": "",
    "isExistingProcess": False,
    "setPermission": False,
    "owners": [
    ],
    "isUserImpersonate": False
  }
}

CLUSTER_IDLE_TIMEOUT = env('CLUSTER_IDLE_TIMEOUT')

SPARK_INTERPRETER_SELECTION = env('SPARK_INTERPRETER_SELECTION')

CORE_SCALING_MIN_CAPACITY = env('CORE_SCALING_MIN_CAPACITY')

CORE_SCALING_MAX_CAPACITY = env('CORE_SCALING_MAX_CAPACITY')

CORE_SCALING_ADJUSTMENT_TYPE = env('CORE_SCALING_ADJUSTMENT_TYPE')

CORE_SCALING_ADJUSTMENT = env('CORE_SCALING_ADJUSTMENT')

CORE_SCALING_COOLDOWN_PERIOD = env('CORE_SCALING_COOLDOWN_PERIOD')

TASK_SCALING_MIN_CAPACITY = env('TASK_SCALING_MIN_CAPACITY')

TASK_SCALING_MAX_CAPACITY = env('TASK_SCALING_MAX_CAPACITY')

TASK_SCALING_ADJUSTMENT_TYPE = env('TASK_SCALING_ADJUSTMENT_TYPE')

TASK_SCALING_ADJUSTMENT = env('TASK_SCALING_ADJUSTMENT')

TASK_SCALING_COOLDOWN_PERIOD = env('TASK_SCALING_COOLDOWN_PERIOD')

AUTO_SCALING_ROLE = env('AUTO_SCALING_ROLE')

SCALE_DOWN_BEHAVIOUR = env('SCALE_DOWN_BEHAVIOUR')

MAX_CAPACITY_UNITS = env('MAX_CAPACITY_UNITS')

MAX_CORE_CAPACITY_UNITS = env('MAX_CORE_CAPACITY_UNITS')

MAX_DEMAND_CAPACITY_UNITS = env('MAX_DEMAND_CAPACITY_UNITS')

MIN_CAPACITY_UNITS = env('MIN_CAPACITY_UNITS')

UNIT_TYPE = env('UNIT_TYPE')

SHUFFLE_SERVICE = env('SHUFFLE_SERVICE')

SHUFFLE_PARTITIONS = env('SHUFFLE_PARTITIONS')

DEFAULT_PARALLELISM = env('DEFAULT_PARALLELISM')

STEP_CONCURRENCY_LEVEL = env('STEP_CONCURRENCY_LEVEL')

DEFAULTS_EXECUTOR_CORES = env('DEFAULTS_EXECUTOR_CORES')

DEFAULTS_EXECUTOR_MEMORY = env('DEFAULTS_EXECUTOR_MEMORY')

DEFAULTS_EXECUTOR_INSTANCES = env('DEFAULTS_EXECUTOR_INSTANCES')

DEFAULTS_DRIVER_MEMORY = env('DEFAULTS_DRIVER_MEMORY')

DEFAULTS_DRIVER_CORES = env('DEFAULTS_DRIVER_CORES')

INTERPRETER_LIFECYCLE_THRESHOLD = env('INTERPRETER_LIFECYCLE_THRESHOLD')

INTERPRETER_LIFECYCLE_CHECKINTERVAL = env('INTERPRETER_LIFECYCLE_CHECKINTERVAL')
