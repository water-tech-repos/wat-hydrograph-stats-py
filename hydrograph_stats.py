import pandas as pd

import argparse
from datetime import datetime
from dateutil import tz
import fsspec
from io import StringIO
import json
from os import PathLike
import sys
from typing import Optional, Tuple, Union


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


def hydrograph_max(df: pd.DataFrame, col_datetime: str, col_flow: str) -> Tuple[float, datetime]:
    max_idx = df[col_flow].idxmax()
    max_flow = float(df[col_flow].iloc[max_idx])
    max_datetime = df[col_datetime].iloc[max_idx].to_pydatetime()
    return max_flow, max_datetime


def hydrograph_min(df: pd.DataFrame, col_datetime: str, col_flow: str) -> Tuple[float, datetime]:
    min_idx = df[col_flow].idxmin()
    min_flow = float(df[col_flow].iloc[min_idx])
    min_datetime = df[col_datetime].iloc[min_idx].to_pydatetime()
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


def main(hydrograph: Union[str, PathLike, StringIO], storage_options: Optional[dict], duration: str, sep: str,
         col_idx_dt: int, col_idx_q: int, usgs_rdb: bool, pretty_print: bool,
         out: Optional[str], out_fsspec_kwargs: Optional[dict]):
    if usgs_rdb:
        df = read_usgs_rdb(hydrograph, storage_options)
        col_datetime = USGS_COL_DATETIME
        col_flow = get_usgs_flow_col(df)
    else:
        df = pd.read_csv(hydrograph, sep=sep, storage_options=storage_options)
        col_datetime = df.columns[col_idx_dt]
        col_flow = df.columns[col_idx_q]
        df[col_datetime] = pd.to_datetime(df[col_datetime], infer_datetime_format=True)
    result = analyze_hydrograph(df, col_datetime, col_flow, duration)
    indent = 2 if pretty_print else None
    output = json.dumps(result, indent=indent)
    print(output)
    if out:
        out_kwargs = out_fsspec_kwargs if out_fsspec_kwargs else {}
        with fsspec.open(out, 'w', **out_kwargs) as o:
            o.write(output)
            o.write('\n')
    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('hydrograph', default=sys.stdin, nargs='?', help='Path or URL to hydrograph.')
    parser.add_argument('--storage-options', default=DEFAULT_STORAGE_OPTIONS, type=json.loads,
                        help=f"Storage options for hydrograph location; passed to pandas.read_csv. JSON. Default: {DEFAULT_STORAGE_OPTIONS}")
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
    args = parser.parse_args()
    main(args.hydrograph, args.storage_options, args.duration, args.sep, args.col_idx_dt, args.col_idx_q, args.usgs_rdb, args.pretty_print, args.out, args.out_fsspec_kwargs)
