import boto3
from botocore.exceptions import ClientError

import string
import random
from . import mail_constants as mc

region_name = mc.email_region_name
role_arn = mc.email_role_arn
sender = mc.email_sender
source_arn = mc.email_source_arn


class AWSMailSTS:

    def get_sts_session(self):
        chars_list = '{}{}{}'
        chars_list = chars_list.format(
            string.ascii_uppercase,
            string.digits,
            string.ascii_lowercase
        )
        return ''.join(random.choice(chars_list) for _ in range(20))

    def get_sts_role(self):
        session_name = self.get_sts_session()
        sts_client = boto3.client('sts', region_name=region_name)
        sts = sts_client.assume_role(
            RoleArn=role_arn,
            RoleSessionName=session_name,
            DurationSeconds=1000
        )
        return sts

    def get_credentials_for_role(self, role):
        return {
            'accesskey_id': role['Credentials']['AccessKeyId'],
            'secret_access_key': role['Credentials']['SecretAccessKey'],
            'session_token': role['Credentials']['SessionToken']
        }


class AWSMailSES:

    def __init__(self):
        sts = AWSMailSTS()
        role = sts.get_sts_role()
        credentials = sts.get_credentials_for_role(role)
        self.client = self.create_client(credentials)

    def create_client(self, credentials):
        return boto3.client('ses',
                            region_name=region_name,
                            aws_access_key_id=credentials.get("accesskey_id"),
                            aws_secret_access_key=credentials.get("secret_access_key"),
                            aws_session_token=credentials.get("session_token"),
                            )

    def send_mail(self, to_address=[], subject="", body=""):
        try:
            response = self.client.send_email(
                Destination={
                    'ToAddresses': to_address,
                },
                Message={
                    'Body': {
                        'Html': {
                            'Charset': 'UTF-8',
                            'Data': body,
                        },
                    },
                    'Subject': {
                        'Charset': 'UTF-8',
                        'Data': subject,
                    },
                },
                SourceArn=source_arn,
                Source=sender,
            )
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            print("Email sent! Message ID:"),
            print(response['ResponseMetadata']['RequestId'])
