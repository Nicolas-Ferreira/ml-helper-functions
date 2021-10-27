import sagemaker
import boto3


sess = sagemaker.Session()
role = sagemaker.get_execution_role()

# Define container
container = sagemaker.image_uris.retreive(
    framework = 'xgboost',
    region = boto3.Session().region_name,
    version = '1.0-1'
)

# S3 buckets with data
# target, feature1, feature2, ...
train_path = ''
test_path = ''
output_path = ''

# Define s3 inputs
s3_input_train = sagemaker.inputs.TrainingInput(
    s3_data = train_path,
    content_type = 'parquet'
)
s3_input_test = sagemaker.inputs.TrainingInput(
    s3_data = test_path,
    content_type = 'parquet'
)

# Define Estimator
xgb = sagemaker.estimator.Estimator(
    container,
    role,
    instance_count = 1,
    instance_type = 'ml.r5.4xlarge'
    output_path = output_path,
    sagemaker_session = sess
)

# Set Hyperparameters
xgb.set_hyperparameters(
    max_depth = 5,
    eta = 0.2,
    gamma = 4,
    min_child_weight = 6,
    subsample = 0.8,
    silent = 0,
    objective = 'binary:logistic',
    num_round = 100
)

# Fit model
xgb.fit(
    {
        'train': s3_input_train,
        'validation': s3_input_test
    }
)
