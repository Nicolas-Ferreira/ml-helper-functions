import boto3

client = boto3.client('sagemaker')

client.start_pipeline_execution(
    PipelineName = 'pipeline-name'
)
