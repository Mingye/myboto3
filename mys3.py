import boto3, tempfile, re
from datetime import datetime, timezone, timedelta
from dateutil import parser


def assume_role(role):
    sts = boto3.client('sts')
    assumed_role = sts.assume_role(RoleArn=role, RoleSessionName='assumed-role')
    client = boto3.client(
        's3',
        aws_access_key_id = assumed_role['Credentials']['AccessKeyId'],
        aws_secret_access_key = assumed_role['Credentials']['SecretAccessKey'],
        aws_session_token = assumed_role['Credentials']['SessionToken'],
    )
    client.renewal = assumed_role['Credentials']['Expiration'] - timedelta(minutes=5)
    client.role = role
    return client

def validate_role(client):
    if client.renewal < datetime.now(timezone.utc):
        return assume_role(client.role)
    else:
        return client


def download_object(client, bucket, key, file, range_=None):
    if range_ is None:
        response = client.get_object(Bucket=bucket, Key=key)
    else:
        response = client.get_object(Bucket=bucket, Key=key, Range=range_)
    file.write(response['Body'].read())
    file.seek(0)

def download_upload_object(client1, bucket1, key1, client2, bucket2, key2, range_=None):
    with tempfile.TemporaryFile() as tf:
        download_object(client1, bucket1, key1, tf, range_)
        client2.upload_fileobj(tf, bucket2, key2)


ONGOING_TEMPLATE = re.compile(r'ongoing-request="(?P<ongoing>true|false)"')
EXPIRY_TEMPLATE = re.compile(r'expiry-date="(?P<expiry>.+)"')

def get_glacier_metadata(client, bucket, key):
    response = client.head_object(Bucket=bucket, Key=key)
    size = response['ContentLength']
    if 'StorageClass' in response:
        storage = response['StorageClass']
        if 'Restore' in response:
            restore = response['Restore']
            ongoing = ONGOING_TEMPLATE.search(restore).group('ongoing') == 'true'
            if not ongoing:
                expiry = parser.parse(EXPIRY_TEMPLATE.search(restore).group('expiry'))
                return size, storage, ongoing, expiry
            else:
                return size, storage, ongoing, None
        else:
            return size, storage, False, None
    else:
        return size, 'STANDARD', False, None

def get_object_size(client, bucket, key):
    return client.head_object(Bucket=bucket, Key=key)['ContentLength']


def _extract_fields_from_response(response, fields):
    return [tuple(content.get(field) for field in fields)
            for content in response['Contents']]

def list_objects(client, bucket, prefix, fields):
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    results = _extract_fields_from_response(response, fields)
    while response['IsTruncated']:
        nct = response['NextContinuationToken']
        response = client.list_objects_v2(Bucket=bucket, Prefix=prefix,
                                          ContinuationToken=nct)
        results.extend(_extract_fields_from_response(response, fields))
    return results
