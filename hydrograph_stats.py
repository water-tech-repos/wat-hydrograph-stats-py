#!/usr/bin/env python3

from dataclasses import dataclass
import pandas as pd
import fsspec
import yaml

import argparse
from dataclasses import field
from dateutil import tz
from io import StringIO
import json
from os import PathLike
import sys
from typing import List, Optional, Tuple, Union


DEFAULT_HYDROGRAPHS = []
DEFAULT_WAT_PAYLOAD = None
DEFAULT_WAT_PAYLOAD_FSSPEC_KWARGS = None
DEFAULT_CONFIG = None
DEFAULT_CONFIG_FSSPEC_KWARGS = None
DEFAULT_STORAGE_OPTIONS = None
DEFAULT_DURATION = '3H'
DEFAULT_SEP = ','
DEFAULT_COL_IDX_DT = 0
DEFAULT_COL_IDX_Q = 1
DEFAULT_USGS_RDB = False
DEFAULT_PRETTY_PRINT = False
DEFAULT_OUT = None
DEFAULT_OUT_FSSPEC_KWARGS = None

USGS_SEP = '\t'
USGS_COL_DATETIME = 'datetime'
USGS_COL_FLOW_ENDSWITH = '00060'
USGS_COL_TZ = 'tz_cd'
USGS_TZ_MAPPINGS = {
    'EST': 'America/New_York',
    'EDT': 'America/New_York',
    'CST': 'America/Chicago',
    'CDT': 'America/Chicago',
    'MST': 'America/Denver',
    'MST': 'America/Denver',
    'PST': 'America/Los_Angeles',
    'PDT': 'America/Los_Angeles',
    'HST': 'America/Honolulu',
    'HDT': 'America/Honolulu',
    'AKST': 'America/Anchorage',
    'AKDT': 'America/Anchorage',
    'AST': 'America/Puerto_Rico',
}


def hydrograph_max(df: pd.DataFrame, col_datetime: str, col_flow: str) -> Tuple[float, pd.Timestamp]:
    max_idx = df[col_flow].idxmax()
    max_flow = float(df[col_flow].iloc[max_idx])
    max_datetime = df[col_datetime].iloc[max_idx]
    return max_flow, max_datetime


def hydrograph_min(df: pd.DataFrame, col_datetime: str, col_flow: str) -> Tuple[float, pd.Timestamp]:
    min_idx = df[col_flow].idxmin()
    min_flow = float(df[col_flow].iloc[min_idx])
    min_datetime = df[col_datetime].iloc[min_idx]
    return min_flow, min_datetime


def analyze_hydrograph(df: pd.DataFrame, col_datetime: str, col_flow: str, duration: str) -> dict:
    max_flow, max_datetime = hydrograph_max(df, col_datetime, col_flow)
    min_flow, min_datetime = hydrograph_min(df, col_datetime, col_flow)
    avg = df[col_flow].mean()

    df_dt_q = df[[col_datetime, col_flow]]
    df_rolling = df_dt_q.rolling(window=duration, on=col_datetime).mean()
    duration_max, duration_max_datetime = hydrograph_max(df_rolling, col_datetime, col_flow)
    duration_min, duration_min_datetime = hydrograph_min(df_rolling, col_datetime, col_flow)

    return {
        'max': max_flow,
        'max_datetime': max_datetime.isoformat(),
        'min': min_flow,
        'min_datetime': min_datetime.isoformat(),
        'avg': avg,
        'duration': duration,
        'duration_max': duration_max,
        'duration_max_datetime': duration_max_datetime.isoformat(),
        'duration_min': duration_min,
        'duration_min_datetime': duration_min_datetime.isoformat(),
    }


def get_usgs_tz(tz_cd: str):
    return tz.gettz(USGS_TZ_MAPPINGS.get(tz_cd))


def localize_usgs_datetime(row: pd.Series) -> pd.Series:
    tzinfo = USGS_TZ_MAPPINGS.get(row[USGS_COL_TZ])
    return row[USGS_COL_DATETIME].tz_localize(tzinfo)


def get_usgs_flow_col(df: pd.DataFrame) -> str:
    col_idx_flow = list(df.columns.str.endswith(USGS_COL_FLOW_ENDSWITH)).index(True)
    col_flow =  df.columns[col_idx_flow]
    return col_flow


def read_usgs_rdb(hydrograph: Union[str, PathLike, StringIO], storage_options: dict) -> pd.DataFrame:
    df = pd.read_table(hydrograph, sep=USGS_SEP, comment='#', header=[0, 1], storage_options=storage_options)
    df.columns = df.columns.droplevel(1)
    df[USGS_COL_DATETIME] = pd.to_datetime(df[USGS_COL_DATETIME], infer_datetime_format=True)
    df[USGS_COL_DATETIME] = df.apply(localize_usgs_datetime, axis=1)
    return df


def load_yaml(path: str, fsspec_kwargs: Optional[dict] = None) -> dict:
    with fsspec.open(path, 'r', **fsspec_kwargs) as conf:
        return yaml.load(conf.read(), yaml.Loader)


@dataclass
class HydrographStatsConfig:
    hydrographs: List[str] = field(default_factory=list)
    storage_options: Optional[dict] = DEFAULT_STORAGE_OPTIONS
    duration: str = DEFAULT_DURATION
    sep: str = DEFAULT_SEP
    col_idx_dt: int = DEFAULT_COL_IDX_DT
    col_idx_q: int = DEFAULT_COL_IDX_Q
    usgs_rdb: bool = DEFAULT_USGS_RDB
    pretty_print: bool = DEFAULT_PRETTY_PRINT
    out: Optional[str] = DEFAULT_OUT
    out_fsspec_kwargs: Optional[dict] = DEFAULT_OUT_FSSPEC_KWARGS

    @classmethod
    def from_dict(cls, d: dict) -> 'HydrographStatsConfig':
        config = cls()
        config.hydrographs = d.get('hydrographs', DEFAULT_HYDROGRAPHS)
        config.storage_options = d.get('storage_options', DEFAULT_STORAGE_OPTIONS)
        config.duration = d.get('duration', DEFAULT_DURATION)
        config.sep = d.get('sep', DEFAULT_SEP)
        config.col_idx_dt = d.get('col_idx_dt', DEFAULT_COL_IDX_DT)
        config.col_idx_q = d.get('col_idx_q', DEFAULT_COL_IDX_Q)
        config.usgs_rdb = d.get('usgs_rdb', DEFAULT_USGS_RDB)
        config.pretty_print = d.get('pretty_print', DEFAULT_PRETTY_PRINT)
        config.out = d.get('out', DEFAULT_OUT)
        config.out_fsspec_kwargs = d.get('out_fsspec_kwargs', DEFAULT_OUT_FSSPEC_KWARGS)
        return config

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> 'HydrographStatsConfig':
        d = vars(args)
        return cls.from_dict(d)

    @classmethod
    def from_yaml(cls, config_yaml: str, fsspec_kwargs: Optional[dict] = None) -> 'HydrographStatsConfig':
        config_dict = load_yaml(config_yaml, fsspec_kwargs)
        return cls.from_dict(config_dict)


@dataclass
class WatModelLink:
    name: str
    source: str
    parameter: str
    format: str

    @classmethod
    def from_dict(cls, d: dict) -> 'WatModelLink':
        name = d['name']
        source = d['source']
        parameter = d['parameter']
        format = d['format']
        return cls(name, source, parameter, format)


@dataclass
class WatEventConfig:
    output_destination: str
    realization_index: int
    realization_seed: int
    event_index: int
    event_seed: int
    starttime: pd.Timestamp
    endtime: pd.Timestamp


    @classmethod
    def from_dict(cls, d: dict) -> 'WatEventConfig':
        output_destination: str = d['output_destination']
        realization_index: int = d['realization']['index']
        realization_seed: int = d['realization']['seed']
        event_index: int = d['event']['index']
        event_seed: int = d['event']['seed']
        starttime = pd.Timestamp(d['time_window']['starttime'])
        endtime = pd.Timestamp(d['time_window']['endtime'])
        return cls(output_destination, realization_index, realization_seed,
                   event_index, event_seed, starttime, endtime)


@dataclass
class WatPayload:
    model_configuration_paths: List[str]
    linked_inputs: List[WatModelLink]
    # required_outputs: List[WatModelLink]
    event_config: WatEventConfig

    @classmethod
    def from_dict(cls, d: dict) -> 'WatPayload':
        model_configuration_paths: List[str] = d['model_configuration']['model_configuration_paths']
        linked_inputs: List[WatModelLink] = [WatModelLink.from_dict(i) for i in d['model_links']['linked_inputs']]
        # required_outputs: List[WatModelLink] = [WatModelLink.from_dict(i) for i in d['model_links']['required_outputs']]
        event_config: WatEventConfig = WatEventConfig.from_dict(d['event_config'])
        # return cls(model_configuration_paths, linked_inputs, required_outputs, event_config)
        return cls(model_configuration_paths, linked_inputs, event_config)

    @classmethod
    def from_yaml(cls, config_yaml: str, fsspec_kwargs: Optional[dict] = None) -> 'WatPayload':
        print(fsspec_kwargs)
        config_dict = load_yaml(config_yaml, fsspec_kwargs)
        return cls.from_dict(config_dict)


def analyze(config: HydrographStatsConfig, wat_payload: Optional[WatPayload] = None) -> dict:
    if wat_payload:
        hydrographs = [h.source for h in wat_payload.linked_inputs]
        out = wat_payload.event_config.output_destination
    else:
        hydrographs = config.hydrographs
        out = config.out
    results = []
    for hydrograph in hydrographs:
        if config.usgs_rdb:
            df = read_usgs_rdb(hydrograph, config.storage_options)
            col_datetime = USGS_COL_DATETIME
            col_flow = get_usgs_flow_col(df)
        else:
            df = pd.read_csv(hydrograph, sep=config.sep, parse_dates=[config.col_idx_dt],
                            storage_options=config.storage_options)
            col_datetime = df.columns[config.col_idx_dt]
            col_flow = df.columns[config.col_idx_q]
            df[col_datetime] = pd.to_datetime(df[col_datetime], infer_datetime_format=True)
        result = analyze_hydrograph(df, col_datetime, col_flow, config.duration)
        result['hydrograph'] = hydrograph
        results.append(result)
    indent = 2 if config.pretty_print else None
    output = json.dumps(results, indent=indent)
    print(output)
    if out:
        out_kwargs = config.out_fsspec_kwargs if config.out_fsspec_kwargs else {}
        with fsspec.open(out, 'w', **out_kwargs) as o:
            o.write(output)
            o.write('\n')
    return results


def parse_args(raw_args: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('hydrographs', default=DEFAULT_HYDROGRAPHS, nargs='*', help='Paths or URLs to hydrographs.')
    parser.add_argument('--wat-payload', default=DEFAULT_WAT_PAYLOAD, help='WAT payload file (YAML).')
    parser.add_argument('--wat-payload-fsspec-kwargs', default=DEFAULT_WAT_PAYLOAD_FSSPEC_KWARGS, type=json.loads,
                        help=f"Extra options passed to fsspec.open to read WAT payload file. JSON. Default: {DEFAULT_WAT_PAYLOAD_FSSPEC_KWARGS}")
    parser.add_argument('--config', default=DEFAULT_CONFIG, help='Configuration file (YAML).')
    parser.add_argument('--config-fsspec-kwargs', default=DEFAULT_CONFIG_FSSPEC_KWARGS, type=json.loads,
                        help=f"Extra options passed to fsspec.open to read config file. JSON. Default: {DEFAULT_CONFIG_FSSPEC_KWARGS}")
    parser.add_argument('--storage-options', default=DEFAULT_STORAGE_OPTIONS, type=json.loads,
                        help=f"Storage options for hydrographs. JSON. Default: {DEFAULT_STORAGE_OPTIONS}")
    parser.add_argument('--duration', default="3H",
                        help=(f'Duration string specifying a rolling window for analysis. Default: "{DEFAULT_DURATION}". '
                              'See: https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases'))
    parser.add_argument('--sep', default=DEFAULT_SEP, help=f'Column separator. Default: "{DEFAULT_SEP}"')
    parser.add_argument('--col-idx-dt', default=DEFAULT_COL_IDX_DT, help=f'Datetime column index. Default: {DEFAULT_COL_IDX_DT}')
    parser.add_argument('--col-idx-q', default=DEFAULT_COL_IDX_Q, help=f'Flow column index. Default: {DEFAULT_COL_IDX_Q}')
    parser.add_argument('--usgs-rdb', action='store_true', default=DEFAULT_USGS_RDB,
                        help=f'Hydrograph in USGS RDB format. Overrides column and sep options. Default: {DEFAULT_USGS_RDB}')
    parser.add_argument('--pretty-print', action='store_true', default=DEFAULT_PRETTY_PRINT, help=f'Pretty print JSON results. Default: {DEFAULT_PRETTY_PRINT}')
    parser.add_argument('--out', default=DEFAULT_OUT, help=f"Output location. Default: {DEFAULT_OUT}")
    parser.add_argument('--out-fsspec-kwargs', default=DEFAULT_OUT_FSSPEC_KWARGS, type=json.loads,
                        help=f"Extra options passed to fsspec.open for writing results. JSON. Default: {DEFAULT_OUT_FSSPEC_KWARGS}")
    args = parser.parse_args(raw_args)
    return args


def main(args: List[str]):
    parsed_args = parse_args(args)
    print(parsed_args)
    if parsed_args.wat_payload:
        wat_payload = WatPayload.from_yaml(parsed_args.wat_payload, parsed_args.wat_payload_fsspec_kwargs)
        config = HydrographStatsConfig.from_yaml(wat_payload.model_configuration_paths[0],
                                                 parsed_args.config_fsspec_kwargs)
    elif parsed_args.config:
        wat_payload = None
        config = HydrographStatsConfig.from_yaml(parsed_args.config, parsed_args.config_fsspec_kwargs)
    else:
        wat_payload = None
        config = HydrographStatsConfig.from_args(parsed_args)
    return analyze(config, wat_payload)


if __name__ == '__main__':
    main(sys.argv[1:])
