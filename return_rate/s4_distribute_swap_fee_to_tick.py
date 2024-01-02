import numpy as np
import os
import pandas as pd
from dataclasses import dataclass
from datetime import datetime, timedelta
from tqdm import tqdm

from config import config, steps
from utils import format_date, get_time_index, load_daily_file



def get_tick_width(max_tick, min_tick):
    return int(max_tick - min_tick + 1)


def get_tick_index(tick, min_tick):
    return int(tick - min_tick)


def process_day_swap(df: pd.DataFrame, min_tick, max_tick):
    arr_0 = np.zeros((1440, get_tick_width(max_tick, min_tick)))
    arr_1 = np.zeros((1440, get_tick_width(max_tick, min_tick)))
    for index, row in df.iterrows():
        time_index = get_time_index(row["block_timestamp"])
        if row["amount0"] > 0:
            v0 = float(
                config["pool_fee_rate"] * row["amount0"] / 10 ** config["decimal0"]
            )
            v1 = 0
        else:
            v0 = 0
            v1 = float(
                config["pool_fee_rate"] * row["amount1"] / 10 ** config["decimal1"]
            )
        arr_0[time_index][get_tick_index(row["current_tick"], min_tick)] += v0
        arr_1[time_index][get_tick_index(row["current_tick"], min_tick)] += v1
    day = df.head(1).iloc[0]["block_timestamp"].date()
    time_serial = pd.date_range(
        datetime.combine(day, datetime.min.time()),
        datetime.combine(day + timedelta(days=1), datetime.min.time())
        - timedelta(minutes=1),
        freq="1T",
    )

    day_fee_0 = pd.DataFrame(arr_0, index=time_serial)
    day_fee_0.to_csv(
        os.path.join(config["path"], steps["s4"]["path"], f"{day_str}_0.csv")
    )
    day_fee_1 = pd.DataFrame(arr_1, index=time_serial)
    day_fee_1.to_csv(
        os.path.join(config["path"], steps["s4"]["path"], f"{day_str}_1.csv")
    )


@dataclass
class tick_range:
    day_str: str
    min_tick: int
    max_tick: int


if __name__ == "__main__":
    start = config["start"]
    day = start
    total_df = None
    start_time = datetime.now()
    end = config["end"]
    day_length = ((end - start).days + 1) * 1440
    tick_range_list = []
    with tqdm(total=(end - start).days + 1, ncols=100) as pbar:
        while day <= end:
            day_str = format_date(day)
            df = load_daily_file(config, day_str)
            df = df[df["tx_type"] == "SWAP"]
            max_tick = df["current_tick"].max()
            min_tick = df["current_tick"].min()
            process_day_swap(df, min_tick, max_tick)
            tick_range_list.append(tick_range(day_str, min_tick, max_tick))
            day = day + timedelta(days=1)
            pbar.update()

    range_df = pd.DataFrame(tick_range_list)
    range_df.to_csv(
        os.path.join(config["path"], steps["s4"]["path"], steps["s4"]["map_file"]),
        index=False,
    )
