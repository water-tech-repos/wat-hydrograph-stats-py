from hydrograph_stats import main
from .resources import *

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import pytest

import json
import os


S3_ENDPOINT = os.getenv('S3_ENDPOINT', 'http://localhost:9990')
AWS_ACCESS_KEY_ID = 'AKIAIOSFODNN7EXAMPLE'
AWS_SECRET_ACCESS_KEY = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
S3_STORAGE_OPTIONS = json.dumps({
    'key': AWS_ACCESS_KEY_ID,
    'secret': AWS_SECRET_ACCESS_KEY,
    'client_kwargs': {
        'endpoint_url': S3_ENDPOINT,
    },
})
S3_BUCKET = 'mybucket'


S3 = boto3.resource('s3', endpoint_url=S3_ENDPOINT,
                    aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                    config=Config(signature_version='s3v4'), region_name='us-east-1')


def setup_module():
    print('--- S3 INTEGRATION TESTS : SETUP ---')
    S3.create_bucket(Bucket=S3_BUCKET)
    S3.Object(S3_BUCKET, HSM1_CSV).put(Body=open(PATH_HSM1_CSV, 'rb'))
    S3.Object(S3_BUCKET, HYDROGRAPH_CSV).put(Body=open(PATH_HYDROGRAPH_CSV, 'rb'))
    S3.Object(S3_BUCKET, HYDROGRAPH_TXT).put(Body=open(PATH_HYDROGRAPH_TXT, 'rb'))
    S3.Object(S3_BUCKET, WAT_PAYLOAD_AWS_YML).put(Body=open(PATH_WAT_PAYLOAD_AWS_YML, 'rb'))
    S3.Object(S3_BUCKET, CONFIG_AWS_YML).put(Body=open(PATH_CONFIG_AWS_YML, 'rb'))


def teardown_module():
    print('--- S3 INTEGRATION TESTS : TEARDOWN ---')
    pass


def s3_object_exists(key: str):
    # https://stackoverflow.com/a/33843019
    try:
        S3.Object(S3_BUCKET, key).load()
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise
    return True


def get_s3_object_content(key: str):
    obj = S3.Object(S3_BUCKET, key)
    return obj.get()['Body'].read()


@pytest.mark.integration
def test_aws_read_csv():
    result = main([
        f's3://{S3_BUCKET}/{HYDROGRAPH_CSV}',
        '--storage-options', S3_STORAGE_OPTIONS,
    ])
    assert result[0]['max'] == 47300.0
    assert result[0]['min'] == 14800.0
    assert result[0]['duration_max'] == 47225.0


@pytest.mark.integration
def test_aws_read_usgs_rdb():
    result = main([
        f's3://{S3_BUCKET}/{HYDROGRAPH_TXT}',
        '--storage-options', S3_STORAGE_OPTIONS,
        '--usgs-rdb'
    ])
    assert result[0]['max'] == 47300.0
    assert result[0]['min'] == 14800.0
    assert result[0]['duration_max'] == 47225.0


@pytest.mark.integration
def test_aws_out():
    main([
        f's3://{S3_BUCKET}/{HYDROGRAPH_CSV}',
        '--storage-options', S3_STORAGE_OPTIONS,
        '--out', f's3://{S3_BUCKET}/results.json',
        '--out-fsspec-kwargs', S3_STORAGE_OPTIONS,
    ])
    assert s3_object_exists('results.json')
    obj_content = get_s3_object_content('results.json')
    result = json.loads(obj_content)
    assert result[0]['max'] == 47300.0
    assert result[0]['min'] == 14800.0
    assert result[0]['duration_max'] == 47225.0


@pytest.mark.integration
def test_azure_wat_payload():
    main([
        '--wat-payload', f's3://{S3_BUCKET}/{WAT_PAYLOAD_AWS_YML}',
        '--wat-payload-fsspec-kwargs', S3_STORAGE_OPTIONS,
        '--config-fsspec-kwargs', S3_STORAGE_OPTIONS,
    ])
    assert s3_object_exists('results-wat.json')
    blob_content = get_s3_object_content('results-wat.json')
    result = json.loads(blob_content)
    assert result[0]['max'] == 9.447773309400784