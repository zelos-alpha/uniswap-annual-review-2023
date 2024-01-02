import os
import pandas as pd
import shutil
from tqdm import tqdm

from config import config, steps

if __name__ == "__main__":
    shutil.copyfile(
        os.path.join(config["path"], steps["s7"]["path"]),
        os.path.join(config["path"], steps["s7"]["original_path"]),
    )

    df = pd.read_csv(
        os.path.join(config["path"], steps["s7"]["original_path"]),
        dtype=object,
    )
    issue_df = df[df["address"] == config["proxy_addr"]]

    nft_transfer_df = pd.read_csv(
        config["nft_transfer_file_path"], engine="pyarrow", dtype_backend="pyarrow"
    )
    with tqdm(total=len(issue_df.index), ncols=150) as pbar:
        for idx, row in issue_df.iterrows():
            if not str(row["position"]).isnumeric():
                print(row)
                pbar.update()
                continue

            rel_transfers = nft_transfer_df[
                (nft_transfer_df["position_id"] == int(row["position"]))
            ]

            if len(rel_transfers.index) == 0:
                raise RuntimeError(row["position"] + " no nft transfer found ")

            first_transfer = rel_transfers.head(1).iloc[0]
            df.loc[idx, "address"] = first_transfer["to"]
            pbar.update()

    df.to_csv(os.path.join(config["path"], steps["s7"]["path"]), index=False)
