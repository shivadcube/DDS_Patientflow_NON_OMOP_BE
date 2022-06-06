# -*- coding: utf-8 -*-

import base64
import boto3
import datetime
import logging
import os
import time

from services.s3 import constants

logger = logging.getLogger('generic.logger')


def generate_aws_credentials():
    logger.info("Generating Temporary AWS Credentials ....")
    try:
        sts_client = boto3.client('sts')
        response = sts_client.assume_role(
            RoleArn=constants.ROLE_ARN,
            RoleSessionName='s3_request_generator_session',
            DurationSeconds=3600
        )
        logger.debug("assume response:" + str(response))
        aws_access_key_id = response["Credentials"]["AccessKeyId"]
        aws_secret_access_key = response["Credentials"]["SecretAccessKey"]
        aws_session_token = response["Credentials"]["SessionToken"]
        result = {"aws_access_key_id": aws_access_key_id,
                  "aws_secret_access_key": aws_secret_access_key,
                  "aws_session_token": aws_session_token}
        return result

    except Exception as e:
        error_message = 'Generating Temporary AWS Credentials failed ' \
                        'due to - {}'.format(e)
        logger.exception(error_message)
        return error_message


def delete_files_from_s3():
    logger.info("deleting files from s3 ....")
    try:
        s3_credentials = generate_aws_credentials()
        s3_resource = boto3.resource('s3',
                                     aws_access_key_id=s3_credentials.get(
                                         "aws_access_key_id"),
                                     aws_secret_access_key=s3_credentials.get(
                                         "aws_secret_access_key"),
                                     aws_session_token=s3_credentials.get(
                                         "aws_session_token"),
                                     )

        bucket = s3_resource.Bucket(constants.S3_BUCKET)
        bucket.objects.filter(Prefix="dev/pf").delete()
        # to delete folder
        return

    except Exception as e:
        error_message = 's3 file Deletion failed due to - {}'.format(e)
        logger.exception(error_message)
        return e


def upload_files_to_s3(file_path, file_name):
    logger.info("Generating Temporary AWS Credentials ....")
    try:
        s3_credentials = generate_aws_credentials()
        s3_resource = boto3.resource('s3',
                                     aws_access_key_id=s3_credentials.get(
                                         "aws_access_key_id"),
                                     aws_secret_access_key=s3_credentials.get(
                                         "aws_secret_access_key"),
                                     aws_session_token=s3_credentials.get(
                                         "aws_session_token"),
                                     )
        millis = int(round(time.time() * 1000))
        file_name = str(file_name).split('.')[0] + str(millis) + '.' + 'csv'
        s3_folder_path = '{}/{}'.format(constants.S3_MAIN_PATH, file_name)
        s3_folder_path = s3_folder_path.replace(' ', '_')
        s3_resource.meta.client.upload_file('{}'.format(file_path),
                                                constants.S3_BUCKET,
                                                s3_folder_path
                                                )

        s3_file_url = constants.S3_FILE_URL.format(constants.S3_BUCKET,
                                                   constants.S3_REGION,
                                                   s3_folder_path)
        s3_file_url_spark = constants.S3_FILE_URL_SPARK.format(
                                                   constants.S3_BUCKET,
                                                   s3_folder_path)
        file_size = get_file_size(s3_file_url_spark)
        res = [s3_file_url, s3_file_url_spark, file_size]
        return res

    except Exception as e:
        error_message = 's3 file uploading failed due to - {}'.format(e)
        logger.exception(error_message)
        return error_message


def get_file_size(s3_path):
    split_path = s3_path.split('/')
    bucket = split_path[2]
    key_path = '/'.join(split_path[3::])
    s3 = boto3.client('s3')
    response = s3.head_object(Bucket=bucket, Key=key_path)
    size = (response['ContentLength']/1024)/1028
    return size
