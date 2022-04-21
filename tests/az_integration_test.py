from hydrograph_stats import main
from .resources import *

from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from azure.core.exceptions import ResourceExistsError
import pytest

import json
import os


AZURE_STORAGE_CONNECTION_STRING = os.getenv(
    "AZ_CONNECTION_STRING",
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)
AZURE_CONTAINER = 'mycontainer'
AZURE_BLOB_SERVICE_CLIENT: BlobServiceClient = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
AZURE_STORAGE_OPTIONS = json.dumps({
    'connection_string': AZURE_STORAGE_CONNECTION_STRING
})


def upload_blob(file_path: str, blob_name: str):
    blob_client = AZURE_BLOB_SERVICE_CLIENT.get_blob_client(container=AZURE_CONTAINER, blob=blob_name)
    with open(file_path, 'rb') as data:
        blob_client.upload_blob(data, overwrite=True)


def blob_exists(blob_name: str):
    blob = BlobClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING, AZURE_CONTAINER, blob_name)
    return blob.exists()


def get_blob_content(blob_name: str):
    blob = BlobClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING, AZURE_CONTAINER, blob_name)
    return blob.download_blob().readall()

def setup_module():
    print('--- AZURE INTEGRATION TESTS : SETUP ---')
    container_client: ContainerClient = AZURE_BLOB_SERVICE_CLIENT.get_container_client(AZURE_CONTAINER)
    try:
        container_client.create_container()
    except ResourceExistsError:
        print(f'Container {AZURE_CONTAINER} already exists.')
    upload_blob(PATH_HSM1_CSV, HSM1_CSV)
    upload_blob(PATH_HYDROGRAPH_CSV, HYDROGRAPH_CSV)
    upload_blob(PATH_HYDROGRAPH_TXT, HYDROGRAPH_TXT)
    upload_blob(PATH_WAT_PAYLOAD_AZ_YML, WAT_PAYLOAD_AZ_YML)
    upload_blob(PATH_CONFIG_AZ_YML, CONFIG_AZ_YML)


def teardown_module():
    print('--- AZURE INTEGRATION TESTS : TEARDOWN ---')
    pass


@pytest.mark.integration
def test_azure_read_csv():
    result = main([
        f'abfs://{AZURE_CONTAINER}/{HYDROGRAPH_CSV}',
        '--storage-options', AZURE_STORAGE_OPTIONS,
    ])
    assert result[0]['max'] == 47300.0
    assert result[0]['min'] == 14800.0
    assert result[0]['duration_max'] == 47225.0


@pytest.mark.integration
def test_azure_read_usgs_rdb():
    result = main([
        f'abfs://{AZURE_CONTAINER}/{HYDROGRAPH_TXT}',
        '--storage-options', AZURE_STORAGE_OPTIONS,
        '--usgs-rdb'
    ])
    assert result[0]['max'] == 47300.0
    assert result[0]['min'] == 14800.0
    assert result[0]['duration_max'] == 47225.0


@pytest.mark.integration
def test_azure_out():
    main([
        f'abfs://{AZURE_CONTAINER}/{HYDROGRAPH_CSV}',
        '--storage-options', AZURE_STORAGE_OPTIONS,
        '--out', f'abfs://{AZURE_CONTAINER}/results.json',
        '--out-fsspec-kwargs', AZURE_STORAGE_OPTIONS,
    ])
    assert blob_exists('results.json')
    blob_content = get_blob_content('results.json')
    result = json.loads(blob_content)
    assert result[0]['max'] == 47300.0
    assert result[0]['min'] == 14800.0
    assert result[0]['duration_max'] == 47225.0


@pytest.mark.integration
def test_azure_wat_payload():
    main([
        '--wat-payload', f'abfs://{AZURE_CONTAINER}/{WAT_PAYLOAD_AZ_YML}',
        '--wat-payload-fsspec-kwargs', AZURE_STORAGE_OPTIONS,
        '--config-fsspec-kwargs', AZURE_STORAGE_OPTIONS,
    ])
    assert blob_exists('results-wat.json')
    blob_content = get_blob_content('results-wat.json')
    result = json.loads(blob_content)
    assert result[0]['max'] == 9.447773309400784
