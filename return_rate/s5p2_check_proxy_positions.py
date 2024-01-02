from datetime import datetime, date
from decimal import Decimal
import os
from tqdm import tqdm
from config import steps, config

import pandas as pd

liq_path = os.path.join(config["save_path"], steps["s5"]["original_path"])

column_blk_time = "blk_time"


def to_decimal(value):
    return Decimal(value) if value else Decimal(0)


if __name__ == "__main__":
    pos_liq_df = pd.read_csv(
        liq_path,
        converters={
            "liquidity": to_decimal,
            "final_amount0": to_decimal,
            "final_amount1": to_decimal,
        },
        dtype={"id": str},
        parse_dates=True,
    )
    len1 = len(pos_liq_df.index)
    pos_liq_df = pos_liq_df[
        (pos_liq_df["tx_type"] == "BURN")
        & (pos_liq_df["final_amount0"] > 0)
        & (pos_liq_df["final_amount1"] > 0)
        ]
    len2 = len(pos_liq_df.index)
    print("empty burn count ", len1 - len2)
    issued = pos_liq_df[
        pos_liq_df["id"].str.contains(config["proxy_addr"])
    ]

    print(issued)
