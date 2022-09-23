# wat-hydrograph-stats-py
Python implementation of a hypothetical WAT plugin to compute basic hydrograph stats.

## Developer Setup
```
$ pyenv install
$ python -m venv venv-wat-hydrograph-stats-py
$ source ./venv-wat-hydrograph-stats-py/bin/activate
(venv-wat-hydrograph-stats-py) $ pip install -r requirements.txt
```
Install pydsstools from the wheel file for your system here https://github.com/gyanz/pydsstools

## Using the Dev Container
Instead of doing the developer setup above, you can choose to develop inside the dev container. This will give you a more consistent environment to where the code will run in production.

1. Clone this repository if you haven't already `git clone https://github.com/water-tech-repos/wat-hydrograph-stats-py`
2. Download and install VSCode if you don't have it https://code.visualstudio.com/
3. Download and install Docker Desktop if you don't have it https://www.docker.com/products/docker-desktop/
4. Install the Remote Development extension pack in VSCode if you don't have it
5. Press ctrl + shift + P and select *Remote-Containers: Rebuild and Reopen Container*

As the container is built, project dependencies will be installed. Once it completes you will be able to develop from inside of the container.

## Usage
### Tests

Build test container and run integration tests:
```
$ ./integration-tests.sh
```


### Script

Help:
```
$ ./hydrograph_stats.py --help
```

Local hydrograph CSV:
```
$ ./hydrograph_stats.py hydrograph.csv
[{"max": 47300.0, "max_datetime": "2022-04-09T01:30:00", "min": 14800.0, "min_datetime": "2022-04-15T13:30:00", "avg": 29998.9010989011, "duration": "3H", "duration_max": 47225.0, "duration_max_datetime": "2022-04-09T03:45:00", "duration_min": 14983.333333333334, "duration_min_datetime": "2022-04-15T13:45:00", "hydrograph": "hydrograph.csv"}]
```

Pretty print:
```
$ ./hydrograph_stats.py hydrograph.csv --pretty-print
[
  {
    "max": 47300.0,
    "max_datetime": "2022-04-09T01:30:00",
    "min": 14800.0,
    "min_datetime": "2022-04-15T13:30:00",
    "avg": 29998.9010989011,
    "duration": "3H",
    "duration_max": 47225.0,
    "duration_max_datetime": "2022-04-09T03:45:00",
    "duration_min": 14983.333333333334,
    "duration_min_datetime": "2022-04-15T13:45:00",
    "hydrograph": "hydrograph.csv"
  }
]
```

Multiple hydrographs:
```
$ ./hydrograph_stats.py hydrograph.csv hsm1.csv --pretty-print
[
  {
    "max": 47300.0,
    "max_datetime": "2022-04-09T01:30:00",
    "min": 14800.0,
    "min_datetime": "2022-04-15T13:30:00",
    "avg": 29998.9010989011,
    "duration": "3H",
    "duration_max": 47225.0,
    "duration_max_datetime": "2022-04-09T03:45:00",
    "duration_min": 14983.333333333334,
    "duration_min_datetime": "2022-04-15T13:45:00",
    "hydrograph": "hydrograph.csv"
  },
  {
    "max": 9.447773309400784,
    "max_datetime": "2018-01-01T16:01:01.000000001-05:00",
    "min": 2.2485700476373864,
    "min_datetime": "2018-01-03T17:01:01.000000001-05:00",
    "avg": 6.084075310536892,
    "duration": "3H",
    "duration_max": 9.447773309400786,
    "duration_max_datetime": "2018-01-01T19:01:01.000000001-05:00",
    "duration_min": 2.305256687493791,
    "duration_min_datetime": "2018-01-03T17:01:01.000000001-05:00",
    "hydrograph": "hsm1.csv"
  }
]
```

Hydrograph from `stdin`:
```
$ cat hydrograph.csv | ./hydrograph_stats.py
```

Specify duration:
```
$ ./hydrograph_stats.py hydrograph.csv --duration 4H15min
```

Hydrograph in USGS RDB format (tab-separated gage data):
```
$ ./hydrograph_stats.py hydrograph.txt --usgs-rdb
```

Hydrograph retrieved from a URL:
```
$ ./hydrograph_stats.py "https://nwis.waterdata.usgs.gov/md/nwis/uv?cb_00060=on&format=rdb&site_no=01646500" --usgs-rdb
```

Hydrograph retrieved from Azure Blob Storage:
```
$ CONNECTION_STRING="abc123..."
$ ./hydrograph_stats.py "abfs://mycontainer/hydrograph.csv" --storage-options "{\"connection_string\": \"${CONNECTION_STRING}\"}"
```

Hydrograph from S3:
```
$ AWS_KEY="abc123..."
$ AWS_SECRET="secret123..."
$ ./hydrograph_stats.py "s3://mybucket/hydrograph.csv" --storage-options "{\"key\": \"${AWS_KEY}\", \"secret\": \"${AWS_SECRET}\"}"
```

Write output to a file:
```
$ ./hydrograph_stats.py hydrograph.csv --out ./results.json
```

Write output to Azure Blob Storage:
```
$ CONNECTION_STRING="abc123..."
$ ./hydrograph_stats.py hydrograph.csv --out "abfs://mycontainer/results.json" --out-fsspec-kwargs "{\"connection_string\": \"${CONNECTION_STRING}\"}"
```

Hydrograph from Redis key/value pair, results written to Redis key:
```
$ ./hydrograph_stats.py "redis://some.redis.host/0#hydrograph.csv" --out "redis://some.redis.host/0#results"
```

Config file:
```
$ ./hydrograph_stats.py --config config.yaml
```

WAT payload YAML:
```
$ ./hydrograph_stats.py --wat-payload wat_payload.yaml
```

WAT payload YAML retrieved from Azure Blob Storage:
```
$ CONNECTION_STRING="abc123..."
$ ./hydrograph_stats.py --wat-payload "abfs://mycontainer/wat_payload.yaml" --wat-payload-fsspec-kwargs "{\"connection_string\": \"${CONNECTION_STRING}\"}"
```

DSS file from S3
```
$ AWS_KEY="abc123..."
$ AWS_SECRET="secret123..."
$ ./hydrograph_stats.py "s3://mybucket/hydrograph.dss:/REGULAR/TIMESERIES/FLOW//1HOUR/Ex1/" --dss --storage-options "{\"key\": \"${AWS_KEY}\", \"secret\": \"${AWS_SECRET}\"}"
```
If time series is irregular you should also use the --irregular flag, otherwise time series data is assumed to be regular.