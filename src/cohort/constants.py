import environ
env = environ.Env()
environ.Env.read_env()

COHORT_SUMMARY = {
    "Total": 510,
    "age": {
        "0-10": 77,
        "11-20": 65,
        "21-30": 39,
        "31-40": 54,
        "41-50": 47,
        "51-60": 82,
        "60+": 145,
        "Undefined": 1
    },
    "gender": {
        "F": 324,
        "M": 186
    }
}

COHORT_FILE_PATH = '/web/static/doc/sample_upload_cohort.csv'

COHORT_FILE_SIZE_IN_MB = env('COHORT_FILE_SIZE_IN_MB')
