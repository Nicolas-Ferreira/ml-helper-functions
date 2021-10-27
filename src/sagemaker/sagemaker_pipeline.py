import boto3
import sagemaker

from sagemaker.spark.processing import PySparkProcessor
from sagemaker.processing import ProcessingInput, ProcessingOutput
from sagemaker.workflow.steps import ProcessingStep
from sagemaker.workflow.pipeline import Pipeline

sagemaker_role = sagemaker.get_execution_role()


# ###### #
# Params #
BUCKET = ''
PREFIX = ''


# ###### #
# Node 1 #
node_1_proc = PySparkProcessor(
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

node_1_run_args = node_1_proc.get_run_args(
    submit_app = 'node_1.py',
    configuration = configuration,
    arguments = [
        "--aws_bucket", BUCKET,
        "--aws_prefix", PREFIX
    ],
    outputs = [
        ProcessingOutput(
            output_name = "node-1-output",
            source = "/opt/ml/processing/output/node-1.parquet",
            destination = f"s3://{BUCKET}/{PREFIX}"
        )
    ]
)

node_1_step = ProcessingStep(
    name = "node-1-step",
    processor = node_1_proc,
    outputs = node_1_run_args.outputs,
    code = node_1_run_args.code,
    job_arguments = node_1_run_args.arguments
)


# ###### #
# Node 2 #
node_2_proc = PySparkProcessor(
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

node_2_run_args = node_2_proc.get_run_args(
    submit_app = 'node_2.py',
    configuration = configuration,
    arguments = [
        "--aws_bucket", BUCKET,
        "--aws_prefix", PREFIX
    ],
    inputs = [
        ProcessingInput(
            source = node_1_step.properties.ProcessingOutputConfig.Outputs[
                "node-1-output"
            ].S3Output.S3Uri,
            destination = "/opt/ml/processing/output"
        )
    ]
)

node_2_step = ProcessingStep(
    name = "node-2-step",
    processor = node_2_proc,
    inputs = node_1_run_args.inputs,
    code = node_2_run_args.code,
    job_arguments = node_2_run_args.arguments
)


# ######## #
# Pipeline #
pipeline_name = 'pipeline-name'
pipeline = Pipeline(
    name = pipeline_name,
    steps = [
        node_1_step,
        node_2_step
    ]
)

pipeline.upsert(role_arn = sagemaker_role)

pipeline.start()
