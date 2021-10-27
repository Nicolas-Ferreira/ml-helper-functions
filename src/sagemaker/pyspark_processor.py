import boto3
import sagemaker

from sagemaker.spark.processing import PySparkProcessor


sm = boto3.Session().client(service_name='sagemaker')
sagemaker_role = sagemaker.get_execution_role()

# ############################ #
# Pyspark Processor definition #
spark_processor = PySparkProcessor(
    base_job_name = 'spark-proc-name',
    framework_version = '2.4',
    role = sagemaker_role,
    instance_count = 1,
    instance_type = 'ml.r5.8xlarge',
    env = {'AWS_DEFAULT_REGION': boto3.Session().region_name},
    max_runtime_in_seconds = 1800
)

configuration = [
    {
        "Classification": "spark-defaults",
        "Properties": {
            "spark.executor.memory": "200g",
            "spark.driver.memory": "200g",
            "spark.executor.cores": "20",
            "spark.cores.memmaxory": "20"
        }
    }
]

# #################################### #
# Launch Pyspark Processor with script #
proc = spark_processor.run(
    submit_app = 'script.py',
    configuration = configuration,
    wait = False
)
