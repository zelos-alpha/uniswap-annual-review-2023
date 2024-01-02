import os
import pandas as pd
import time
from datetime import datetime, timedelta
from tqdm import tqdm

from config import config
from utils import format_date


if __name__ == "__main__":
    transfers = pd.read_csv(
        config["nft_transfer_file_path"], engine="pyarrow", dtype_backend="pyarrow"
    )

    start = config["start"]
    day = start

    while day <= config["end"]:
        day_str = format_date(day)
        start_time = time.time()
        file = os.path.join(
            config["raw_path"],
            f"{config['chain']}-{config['pool']['address']}-{day_str}.tick.csv",
        )

        uni_tx = pd.read_csv(file)
        mint_tx = uni_tx[uni_tx["tx_type"] == "MINT"]
        mint_tx = mint_tx[~pd.isnull(mint_tx["position_id"])]

        with tqdm(total=len(mint_tx.index), ncols=150) as pbar:
            for index, tx in mint_tx.iterrows():
                rel_transfer = transfers[
                    (transfers["position_id"] == int(tx["position_id"]))
                    & (transfers["block_number"] <= tx["block_number"])
                ]
                if len(rel_transfer.index) == 0:
                    raise RuntimeError(
                        day_str + ": no nft transfer found " + tx.transaction_hash
                    )

                rel_transfer = rel_transfer.head(1)
                uni_tx.loc[index, "receipt"] = uni_tx.loc[index, "sender"]
                uni_tx.loc[index, "sender"] = rel_transfer.iloc[0]["to"]
                pbar.update()
        file_to = os.path.join(
            config["updated_path"],
            f"{config['chain']}-{config['pool']['address']}-{day_str}.tick.csv",
        )
        uni_tx.to_csv(file_to, index=False)
        print(day_str, datetime.now())
        day = day + timedelta(days=1)
