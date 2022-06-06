import environ
env = environ.Env()
environ.Env.read_env()

ROLE_ARN = env('ROLE_ARN')

S3_BUCKET = env('S3_BUCKET')

S3_PROJECT_PATH = env('S3_PROJECT_PATH')

S3_MAIN_PATH = env('S3_MAIN_PATH')

S3_REGION = env('S3_REGION')

S3_FILE_URL = 'https://{}.s3-{}.amazonaws.com/{}'

S3_FILE_URL_SPARK = 's3://{}/{}'
