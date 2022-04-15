import pandas as pd

import argparse
from datetime import datetime
import json
from typing import Tuple


DURATION_DEFAULT = '3H'


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

    df_rolling = df.rolling(window=duration, on=col_datetime).mean()
    duration_max, duration_max_datetime = hydrograph_max(df_rolling, col_datetime, col_flow)
    duration_min, duration_min_datetime = hydrograph_min(df_rolling, col_datetime, col_flow)

    return {
        'max': max_flow,
        'max_datetime': max_datetime,
        'min': min_flow,
        'min_datetime': min_datetime,
        'avg': avg,
        'duration': duration,
        'duration_max': duration_max,
        'duration_max_datetime': duration_max_datetime,
        'duration_min': duration_min,
        'duration_min_datetime': duration_min_datetime,
    }


def main(csv: str, duration: str):
    if csv:
        df = pd.read_csv(csv)
        col_datetime = df.columns[0]
        col_flow = df.columns[1]
        df[col_datetime] = pd.to_datetime(df[col_datetime], infer_datetime_format=True)
        result = analyze_hydrograph(df, col_datetime, col_flow, duration)
        print(json.dumps(result, default=str))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv', help="Path to hydrograph in CSV format")
    parser.add_argument('--duration', default="3H",
                        help=(f"Duration string specifying a rolling window for analysis. Default: \"{DURATION_DEFAULT}\". "
                              "See: https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases"))
    args = parser.parse_args()
    main(args.csv, args.duration)
