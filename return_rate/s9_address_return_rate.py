import os
import warnings
from datetime import datetime
from multiprocessing import Pool
from typing import List

import numpy as np
import pandas as pd
from tqdm import tqdm

from utils import get_hour_time
from config import steps, config, final_value_index

# warnings.filterwarnings("error")
warnings.simplefilter(action="ignore", category=pd.errors.PerformanceWarning)


def join_position_name(values):
    return "|".join(list(filter(lambda x: x != "", values)))


def convert_one_address(param):
    address, positions = param
    # if address != "0x5f5815ec8451e4c6913e6437878721a0f08fbb2d":
    # return
    column_list = [
        "total_net_value",
        "return_rate",
        "cumsum_fee0",
        "cumsum_fee1",
        "amount0",
        "amount1",
        "positions",
        "lp_value_with_prev_liq",
    ]
    amount_column_dict = {
        "total_net_value": "net_value",
        "cumsum_fee0": "fee0",
        "cumsum_fee1": "fee1",
        "amount0": "amount0",
        "amount1": "amount1",
        "lp_value_with_prev_liq": "lp_value_with_prev_liq",
    }
    try:
        raw_df_dict = {}
        final_time = datetime(1970, 1, 1)
        final_row = None
        empty_pos_list = []
        for p in positions["position"]:
            path = os.path.join(config["path"], steps["s8"]["path"], f"{p}.csv")
            if not os.path.exists(path):  # mev
                empty_pos_list.append(p)
                continue
            tmp_df = pd.read_csv(str(path), index_col=0, parse_dates=True)
            tmp_final_row = tmp_df.loc[final_value_index]
            tmp_df = tmp_df.drop(index=final_value_index, axis=0)
            max_idx = tmp_df.index.max().to_pydatetime()
            if final_time < max_idx:
                final_time = max_idx
                final_row = tmp_final_row[list(amount_column_dict.keys())]
            elif final_time == max_idx:
                final_row += tmp_final_row[list(amount_column_dict.keys())]
            for c in column_list:
                if c != "positions":
                    raw_df_dict[(c, p)] = tmp_df[c]
            raw_df_dict[("positions", p)] = pd.Series(p, index=tmp_df.index)

            pass

        # address_df = remove_reentries(address_df, pos_list)
        if len(raw_df_dict) < 1:
            print(f"{address} has no available position file, positions")
            print(positions)
            return
        raw_df = pd.concat(
            list(raw_df_dict.values()), keys=list(raw_df_dict.keys()), axis=1
        )
        raw_df["return_rate"] = raw_df["return_rate"].fillna(1)
        raw_df["positions"] = raw_df["positions"].fillna("")
        raw_df = raw_df.fillna(0)  # rest of the columns

        raw_df["net_value_sum"] = raw_df["total_net_value"].sum(axis="columns")

        result_df = pd.DataFrame(index=raw_df.index)
        # add amounts of all positions
        result_df["net_value"] = raw_df["net_value_sum"]
        result_df["fee0"] = raw_df["cumsum_fee0"].sum(axis="columns")
        result_df["fee1"] = raw_df["cumsum_fee1"].sum(axis="columns")
        result_df["amount0"] = raw_df["amount0"].sum(axis="columns")
        result_df["amount1"] = raw_df["amount1"].sum(axis="columns")
        result_df["lp_value_with_prev_liq"] = raw_df["lp_value_with_prev_liq"].sum(
            axis="columns"
        )
        result_df["positions"] = raw_df["positions"].agg(join_position_name, axis=1)
        final_row.rename(amount_column_dict, inplace=True)
        return_rate_sum_list = []
        for p in positions["position"]:
            if p not in empty_pos_list:
                return_rate_sum_list.append(
                    raw_df["return_rate", p]
                    / raw_df["net_value_sum"]
                    * raw_df["total_net_value", p]
                )
        return_rate_sum_df = pd.concat(return_rate_sum_list, axis=1)
        result_df["return_rate"] = return_rate_sum_df.sum(axis="columns")
        result_df["return_rate"] = (
            result_df["return_rate"].replace(np.nan, 1).replace(0, 1)
        )

        if len(result_df.index) > 0:
            result_df = result_df.resample("1H").last()
            result_df["return_rate"] = result_df["return_rate"].fillna(1)
            result_df["net_value"] = result_df["net_value"].fillna(0)
        result_df["cumulate_return_rate"] = result_df["return_rate"].cumprod()
        result_df.loc[final_value_index] = result_df.loc[result_df.index.max()]
        result_df.loc[final_value_index, list(amount_column_dict.values())] = final_row
        result_df.to_csv(
            os.path.join(config["path"], steps["s9"]["path"], f"{address}.csv")
        )
        pass
    except RuntimeWarning as e:
        print("warning address is ", address)
        raise e
    except Exception as e:
        print("error address is ", address)
        print(f"address {address} has positions:")
        print(positions)
        raise e


multi_thread = True

if __name__ == "__main__":
    addr_pos = pd.read_csv(os.path.join(config["path"], steps["s7"]["path"]))
    positions_df = pd.read_csv(
        os.path.join(config["path"], steps["s5"]["path"]),
        parse_dates=["blk_time"],
        engine="pyarrow",
        dtype={
            "final_amount0": float,
            "final_amount1": float,
            "liquidity": float,
            "id": str,
        },
        dtype_backend="pyarrow",
    )
    positions_df["hour_time"] = positions_df["blk_time"].apply(get_hour_time)
    print("preparing position dictionary")
    pos_groupby_id = positions_df.groupby("id")
    pos_groupby_id_dict = {id: pos_info for id, pos_info in pos_groupby_id}
    print("preparing address list")
    addresses = addr_pos.groupby("address")
    addresses = list(addresses)
    print("start calc")
    if multi_thread:
        with Pool(20) as p:
            res = list(
                tqdm(
                    p.imap(convert_one_address, addresses),
                    ncols=120,
                    total=len(addresses),
                )
            )
    else:
        with tqdm(total=len(addresses), ncols=120) as pbar:
            for a, b in addresses:
                convert_one_address((a, b))
                pbar.update()
