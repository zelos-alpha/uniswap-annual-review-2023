import os.path

import pandas as pd
import time
from _decimal import Decimal
from datetime import timedelta
from tqdm import tqdm
from typing import List, Tuple

from config import steps, config
from utils import (
    format_date,
    PositionLiquidity,
    get_pos_key,
    load_daily_file,
)


def find_pos_from_list(
        pos_list: List[PositionLiquidity], key_word
) -> Tuple[int, PositionLiquidity | None]:
    idx = 0
    for pos in pos_list:
        if pos.id == key_word:
            return idx, pos
        idx += 1
    return -1, None


def get_tick_key(tx_row: pd.Series) -> Tuple[int, int]:
    return int(tx_row["tick_lower"]), int(tx_row["tick_upper"])


def process_day(day_tx: pd.DataFrame):
    tmp_tx = None
    try:
        for tx_index, tx in day_tx.iterrows():
            tmp_tx = tx
            pos_key_str = get_pos_key(tx)
            tick_key = get_tick_key(tx)
            amount0 = amount1 = liquidity = Decimal(0)

            match tx["tx_type"]:
                case "MINT":
                    liquidity = tx["liquidity"]
                    amount0 = tx["amount0"]
                    amount1 = tx["amount1"]
                case "BURN":
                    liquidity = Decimal(0) - tx["liquidity"]
                    amount0 = tx["amount0"]
                    amount1 = tx["amount1"]
                case "COLLECT":
                    amount0 = tx["amount0"]
                    amount1 = tx["amount1"]

            position_history.append(
                PositionLiquidity(
                    pos_key_str,
                    tick_key[0],
                    tick_key[1],
                    tx["tx_type"],
                    tx["block_number"],
                    tx["transaction_hash"],
                    tx["pool_log_index"],
                    tx["block_timestamp"],
                    liquidity,
                    amount0,
                    amount1,
                )
            )
    except Exception as e:
        print(day_tx.head(1).iloc[0]["block_timestamp"], tmp_tx["transaction_hash"])
        raise e


if __name__ == "__main__":
    start = config["start"]
    end = config["end"]
    day = start

    position_history: List[PositionLiquidity] = []

    with tqdm(total=(end - start).days + 1, ncols=150) as pbar:
        while day <= end:
            timer_start = time.time()
            day_str = format_date(day)

            df_tx_day = load_daily_file(config, day_str)

            df_tx_day["block_timestamp"] = pd.to_datetime(
                df_tx_day["block_timestamp"], format="%Y-%m-%d %H:%M:%S"
            )
            df_tx_day = df_tx_day[df_tx_day["tx_type"] != "SWAP"]
            process_day(df_tx_day)
            day += timedelta(days=1)
            timer_end = time.time()
            # print(
            #     day_str,
            #     f"{timer_end - timer_start}s",
            #     "current postions key count",
            #     len(current_position.keys()),
            # )
            pbar.update()
    print("generate result, please wait")
    result_df = pd.DataFrame(position_history)
    result_df = result_df.sort_values(["id", "block_number", "log_index"])
    result_df.to_csv(
        os.path.join(config["save_path"], steps["s5"]["original_path"]), index=False
    )
    pass
