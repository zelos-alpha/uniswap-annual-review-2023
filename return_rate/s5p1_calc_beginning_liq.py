import os
import pandas as pd
from datetime import datetime
from decimal import Decimal
from tqdm import tqdm

from config import steps, config

position_liq_path = os.path.join(config["path"], steps["s5"]["original_path"])
result_path = os.path.join(config["path"], steps["s5"]["path"])

start = config["start"]
end = config["end"]

column_blk_time = "blk_time"


def to_decimal(value):
    return Decimal(value) if value else Decimal(0)


if __name__ == "__main__":
    pos_liq_df = pd.read_csv(
        position_liq_path,
        converters={
            "liquidity": to_decimal,
            "final_amount0": to_decimal,
            "final_amount1": to_decimal,
        },
        dtype={"id": str},
        parse_dates=True,
    )
    pos_liq_df[column_blk_time] = pd.to_datetime(pos_liq_df[column_blk_time])
    start = datetime.combine(start, datetime.min.time())
    end = datetime.combine(end, datetime.max.time())

    pos_group = pos_liq_df.groupby("id")
    pos_df_list = []
    pos_added = []
    with tqdm(total=len(pos_group), ncols=120) as pbar:
        for pos_id, pos_df in pos_group:
            # if pos_id != "0x2814e7bc1567cbbc636ccce2dae63052c1f51b4e-197820-200560":
            #     pbar.update()
            #     continue

            pos_df = pos_df[
                (pos_df[column_blk_time] >= start) & (pos_df[column_blk_time] <= end)
                ]

            liq_delta = pos_df["liquidity"].sum()
            if liq_delta < 0:
                pos_df.loc[len(pos_df)] = {
                    "id": pos_id,
                    "lower_tick": pos_df.iloc[0]["lower_tick"],
                    "upper_tick": pos_df.iloc[0]["upper_tick"],
                    "tx_type": "MINT",
                    "block_number": 0,
                    "tx_hash": "",
                    "log_index": 0,
                    column_blk_time: start,
                    "liquidity": 0 - liq_delta,
                    "final_amount0": 0,
                    "final_amount1": 0,
                }
                pos_added.append(pos_id)
            pbar.update()
            pos_df_list.append(pos_df)
    result_df = pd.concat(pos_df_list)
    result_df = result_df.sort_values(["id", column_blk_time])
    result_df.to_csv(result_path, index=False)
    pd.DataFrame(pos_added, columns=["position"]).to_csv(
        os.path.join(config["path"], steps["s5"]["fixed_position_path"]), index=False
    )
    pass
