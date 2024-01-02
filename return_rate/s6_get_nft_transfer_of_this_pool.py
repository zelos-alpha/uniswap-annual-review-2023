import pandas as pd
from tqdm import tqdm
import os
from config import steps, config

if __name__ == "__main__":
    path = config["nft_transfer_file_path"]
    nft_transfers = pd.read_csv(path)

    this_pool_transfer_idx = []
    pos_lq = pd.read_csv(
        os.path.join(config["path"], steps["s5"]["path"]), dtype={"id": str}
    )
    pos_lq1 = pos_lq[pos_lq["id"].str.len() < 40]
    pos_lq1["id"] = pos_lq1["id"].apply(lambda x: int(x))
    all_pos_id = pos_lq1["id"].unique().tolist()
    with tqdm(total=len(nft_transfers.index), ncols=120) as pbar:
        for index, row in nft_transfers.iterrows():
            if (
                    row["position_id"] in all_pos_id
                    and row["from"] != "0x0000000000000000000000000000000000000000"
                    and row["to"] != "0x0000000000000000000000000000000000000000"
            ):
                this_pool_transfer_idx.append(index)

            pbar.update()

    this_pool_nft_transfer: pd.DataFrame = nft_transfers.loc[
        nft_transfers.index[this_pool_transfer_idx]
    ]
    this_pool_nft_transfer.to_csv(
        os.path.join(config["path"], config["s6"]["path"])
    )
