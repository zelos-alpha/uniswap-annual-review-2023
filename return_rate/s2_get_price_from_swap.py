import os
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm

from config import config, steps
from utils import to_int, format_date

def x96_sqrt_to_decimal(sqrt_priceX96):
    price = int(sqrt_priceX96) / 2**96
    tmp = (price**2) * (10 ** (config["decimal0"] - config["decimal1"]))
    return 1 / tmp if config["is_0_base"] else tmp


def datetime2timestamp(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


if __name__ == "__main__":
    start = config["start"]
    day = start
    total_df = None
    start_time = datetime.now()
    count = 0
    end = config["end"]
    price_df = pd.DataFrame()
    with tqdm(total=(end - start).days + 1, ncols=120) as pbar:
        while day <= end:
            day_str = format_date(day)
            file = os.path.join(
                config["updated_path"],
                f"{config['chain']}-{config['pool']['address']}-{day_str}.tick.csv",
            )
            df: pd.DataFrame = pd.read_csv(
                file,
                parse_dates=["block_timestamp"],
                converters={
                    "total_liquidity": to_int,
                    "sqrtPriceX96": to_int,
                },
            )
            df = df[df["tx_type"] == "SWAP"]
            count += len(df.index)
            df["price"] = df["sqrtPriceX96"].apply(x96_sqrt_to_decimal)

            day = day + timedelta(days=1)
            tmp_price_df: pd.DataFrame = df[
                ["block_timestamp", "price", "total_liquidity", "sqrtPriceX96"]
            ].copy()
            # tmp_price_df["timestamp"] = tmp_price_df["block_timestamp"].apply(datetime2timestamp)
            tmp_price_df = tmp_price_df.drop_duplicates(subset="block_timestamp")

            price_df = pd.concat([price_df, tmp_price_df])
            pbar.update()

    price_df: pd.DataFrame = price_df.set_index("block_timestamp")
    print("resampling, please wait")
    price_df = price_df.resample("1T").last().bfill()  # 用末尾的价格代表这一个小时的价格.
    print("save file")
    price_df.to_csv(os.path.join(config["path"], steps["s2"]["path"]), index=True)
    print("Line processed", count)
