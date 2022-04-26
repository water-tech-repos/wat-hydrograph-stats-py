from hydrograph_stats import main
from .resources import *

from redis import Redis
import pytest

import json
import os


REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))


def get_file_content(path):
    return open(path, 'r').read()


def setup_module():
    print('--- REDIS INTEGRATION TESTS : SETUP ---')
    r = Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    r.set(HSM1_CSV, get_file_content(PATH_HSM1_CSV))
    r.set(HYDROGRAPH_CSV, get_file_content(PATH_HYDROGRAPH_CSV))
    r.set(HYDROGRAPH_TXT, get_file_content(PATH_HYDROGRAPH_TXT))
    r.set(WAT_PAYLOAD_REDIS_YML, get_file_content(PATH_WAT_PAYLOAD_REDIS_YML))
    r.set(CONFIG_REDIS_YML, get_file_content(PATH_CONFIG_REDIS_YML))


def teardown_module():
    print('--- REDIS INTEGRATION TESTS : TEARDOWN ---')
    pass


@pytest.mark.integration
def test_redis_read_csv():
    result = main([
        f'redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}#{HYDROGRAPH_CSV}',
    ])
    assert result[0]['max'] == 47300.0
    assert result[0]['min'] == 14800.0
    assert result[0]['duration_max'] == 47225.0


@pytest.mark.integration
def test_redis_read_usgs_rdb():
    result = main([
        f'redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}#{HYDROGRAPH_TXT}',
        '--usgs-rdb'
    ])
    assert result[0]['max'] == 47300.0
    assert result[0]['min'] == 14800.0
    assert result[0]['duration_max'] == 47225.0


@pytest.mark.integration
def test_redis_out():
    main([
        f'redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}#{HYDROGRAPH_CSV}',
        '--out', f'redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}#results',
    ])
    r = Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    assert r.exists('results')
    result = json.loads(r.get('results'))
    assert result[0]['max'] == 47300.0
    assert result[0]['min'] == 14800.0
    assert result[0]['duration_max'] == 47225.0


@pytest.mark.integration
def test_redis_wat_payload():
    main([
        '--wat-payload', f'redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}#{WAT_PAYLOAD_REDIS_YML}',
    ])
    r = Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    assert r.exists('results-wat')
    result = json.loads(r.get('results-wat'))
    assert result[0]['max'] == 9.447773309400784
