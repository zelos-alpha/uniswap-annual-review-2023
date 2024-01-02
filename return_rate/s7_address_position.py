import os.path

import pandas as pd
from datetime import timedelta
from tqdm import tqdm

from config import steps, config
from utils import format_date, get_pos_key, load_daily_file

if __name__ == "__main__":
    start = config["start"]
    end = config["end"]
    day = start

    position_address_dict: dict[str, str] = {}

    with tqdm(total=(end - start).days + 1, ncols=150) as pbar:
        while day <= end:
            day_str = format_date(day)

            df = load_daily_file(config, day_str)

            df_mint = df[df["tx_type"] == "MINT"]

            for index, row in df_mint.iterrows():
                pos_key = get_pos_key(row)
                if pos_key not in position_address_dict:
                    position_address_dict[pos_key] = {
                        "address": row["sender"],
                        "day": day_str,
                        "type": "MINT",
                    }
            df_collect = df[df["tx_type"] == "COLLECT"]
            for index, row in df_collect.iterrows():
                pos_key = get_pos_key(row)
                if pos_key not in position_address_dict:
                    position_address_dict[pos_key] = {
                        "address": row["receipt"],
                        "day": day_str,
                        "type": "BURN",
                    }

            day += timedelta(days=1)
            pbar.update()
    r = []
    for k, v in position_address_dict.items():
        r.append({"position": k, "address": v["address"], "day": v["day"]})
    result = pd.DataFrame.from_dict(r)
    result.to_csv(os.path.join(config["path"], steps["s7"]["path"]), index=False)
