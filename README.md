# wat-hydrograph-stats-py
Python implementation of a hypothetical WAT plugin to compute basic hydrograph stats.

## Developer Setup
```
$ pyenv install
$ python -m venv venv-wat-hydrograph-stats-py
$ source ./venv-wat-hydrograph-stats-py/bin/activate
(venv-wat-hydrograph-stats-py) $ pip install -r requirements.txt
```

## Usage
### Tests

Integration tests:
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

Write output to a file:
```
$ ./hydrograph_stats.py hydrograph.csv --out ./results.json
```

Write output to Azure Blob Storage:
```
$ CONNECTION_STRING="abc123..."
$ ./hydrograph_stats.py hydrograph.csv --out "abfs://mycontainer/results.json" --out-fsspec-kwargs "{\"connection_string\": \"${CONNECTION_STRING}\"}"
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
