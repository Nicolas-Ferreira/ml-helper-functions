import boto3
import sagemaker

from sagemaker.estimator import Estimator
from sagemaker.tuner import IntegerParameter
from sagemaker.tuner import ContinuousParameter
from sagemaker.tuner import HyperparameterTuner


BUCKET = ''
DATA_INPUT = ''
MODEL_OUTPUT = ''

model_path = f"s3://{BUCKET}/{MODEL_OUTPUT}/"

## Datasets
s3_input_train = sagemaker.inputs.TrainingInput(s3_data=f's3://{BUCKET}/{DATA_INPUT}/train.csv', content_type='csv')
s3_input_validation = sagemaker.inputs.TrainingInput(s3_data=f's3://{BUCKET}/{DATA_INPUT}/validation.csv', content_type='csv')

training_instance_type = 'ml.m5.xlarge'
sagemaker_session = sagemaker.session.Session()
role = sagemaker.get_execution_role()
region = boto3.Session().region_name

## Instance parameters
image_uri = sagemaker.image_uris.retrieve(
    framework="xgboost",
    region=region,
    version="1.2-2",
    py_version="py3",
    instance_type=training_instance_type,
)

## Declare estimator
xgb_train = Estimator(
    image_uri=image_uri,
    instance_type=training_instance_type,
    instance_count=1,
    output_path=model_path,
    role=role
)

## Define hyperparameter ranges
hyperparameter_ranges = {
    'alpha': ContinuousParameter(0, 10, scaling_type="Auto"),
    'colsample_bylevel': ContinuousParameter(0.1, 1,scaling_type="Logarithmic"),
    'colsample_bytree': ContinuousParameter(0.5, 1, scaling_type='Logarithmic'),
    'eta': ContinuousParameter(0.1, 0.5, scaling_type='Logarithmic'),
    'gamma':ContinuousParameter(0, 5, scaling_type='Auto'),
    'lambda': ContinuousParameter(0,10,scaling_type='Auto'),
    'max_delta_step': IntegerParameter(0,10,scaling_type='Auto'),
    'max_depth': IntegerParameter(0,10,scaling_type='Auto'),
    'min_child_weight': ContinuousParameter(0,10,scaling_type='Auto'),
    'num_round': IntegerParameter(1,120,scaling_type='Auto'),
    'subsample': ContinuousParameter(0.5,1,scaling_type='Logarithmic')
}

objective_metric_name = 'validation:aucpr'

## Declare tuner
tuner_log = HyperparameterTuner(
    xgb_train,
    objective_metric_name,
    hyperparameter_ranges,
    max_jobs=40,
    max_parallel_jobs=10,
    strategy='Bayesian'
)

## Starts the hyperparameter tuning job
tuner_log.fit(
    {
        'train': s3_input_train, 
        'validation': s3_input_validation
    }, 
    include_cls_metadata=False
)

## Prints the status of the latest hyperparameter tuning job
boto3.client('sagemaker').describe_hyper_parameter_tuning_job(
    HyperParameterTuningJobName=tuner_log.latest_tuning_job.job_name
)['HyperParameterTuningJobStatus']

## Get best training job
tuner_log.best_training_job()
