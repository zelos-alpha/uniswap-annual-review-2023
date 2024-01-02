from multiprocessing import Pool

import numpy as np
import os
import pandas as pd
import sys
from tqdm import tqdm

from config import steps, config, final_value_index
from utils import load_price

debug = True


NET_VALUE_DECIMAL = 3

MIN_NET_VALUE = 10**-NET_VALUE_DECIMAL


def safe_div(a, b):
    if np.abs(b) < MIN_NET_VALUE:
        return np.inf
    else:
        return a / b


def merge_return(liq_file_path):
    address = os.path.basename(liq_file_path).replace(".csv", "")

    if addr_to_check != "" and address != addr_to_check:
        return

    # skip exist files
    # if os.path.exists(os.path.join(os.path.join(config["path"], steps["s10"]["path"], address + ".csv")):
    #     return

    liq_df = pd.read_csv(
        liq_file_path,
        parse_dates=True,
        index_col=0,
        dtype={"positions": object},
    )
    try:
        liq_df.loc[
            liq_df.index[len(liq_df.index) - 2], "lp_value_with_prev_liq"
        ] = liq_df.loc[final_value_index]["lp_value_with_prev_liq"]
        liq_df = liq_df[liq_df.index < final_value_index]

        cash_file_path = os.path.join(config["cash_path"], address + ".csv")
        if not os.path.exists(cash_file_path):
            return
        cash_df = pd.read_csv(cash_file_path, parse_dates=True, index_col=0)

        start = liq_df.head(1).index[0]
        end = liq_df.tail(1).index[0]


        liq_df = liq_df[(liq_df.index >= start) & (liq_df.index <= end)]
        liq_df["cumulate_return_rate"] = liq_df["return_rate"].cumprod()

        cash_df["weth"] = cash_df["weth"] + cash_df["eth"]
        cash_df["weth"] = cash_df["weth"].apply(lambda x: 0 if x < 1 / 10**18 else x)
        cash_df["usdc"] = cash_df["usdc"].apply(lambda x: 0 if x < 1 / 10**6 else x)

        merged_index_start = liq_df.index[0]
        merged_index_end = end
        merged_df = pd.DataFrame(
            index=pd.date_range(merged_index_start, merged_index_end, freq="1H")
        )
        merged_df[
            [
                "lp_net_value",
                "lp_return_rate",
                "lp_cumulate_return_rate",
                "positions",
                "lp_fee0",
                "lp_fee1",
                "lp_amount0",
                "lp_amount1",
                "lp_value_with_prev_liq",
            ]
        ] = liq_df[
            [
                "net_value",
                "return_rate",
                "cumulate_return_rate",
                "positions",
                "fee0",
                "fee1",
                "amount0",
                "amount1",
                "lp_value_with_prev_liq",
            ]
        ]

        merged_df = merged_df.fillna(
            {
                "lp_net_value": 0,
                "lp_return_rate": 1,
                "lp_fee0": 0,
                "lp_fee1": 0,
                "lp_amount0": 0,
                "lp_amount1": 0,
                "lp_value_with_prev_liq": 0,
            }
        )
        merged_df["lp_cumulate_return_rate"] = merged_df[
            "lp_cumulate_return_rate"
        ].ffill()

        merged_df[["cash_amount0", "cash_amount1"]] = cash_df[["usdc", "weth"]]
        merged_df["price"] = price_df[
            (price_df.index >= merged_index_start)
            & (price_df.index <= merged_index_end)
        ]["price"]
        merged_df[["cash_amount0", "cash_amount1"]] = merged_df[
            ["cash_amount0", "cash_amount1"]
        ].fillna(0)
        merged_df["cash_net_value"] = (
            merged_df["cash_amount1"] * merged_df["price"] + merged_df["cash_amount0"]
        )
        merged_df["net_value"] = merged_df["cash_net_value"] + merged_df["lp_net_value"]

        merged_df["suppose_net_value"] = (
            (merged_df["cash_amount1"].shift(1).bfill() + merged_df["lp_fee1"])
            * merged_df["price"]
            + (merged_df["cash_amount0"].shift(1).bfill() + merged_df["lp_fee0"])
            + merged_df["lp_value_with_prev_liq"]
        )
        merged_df[["suppose_net_value", "net_value"]] = merged_df[
            ["suppose_net_value", "net_value"]
        ].round(NET_VALUE_DECIMAL)

        merged_df["diff"] = merged_df.apply(
            lambda x: x["suppose_net_value"] - x["net_value"]
            if np.abs(x["suppose_net_value"] - x["net_value"]) >= MIN_NET_VALUE
            else 0,
            axis=1,
        )

        merged_df["return_rate"] = (
            merged_df["net_value"] / merged_df["net_value"].shift(1).bfill()
        )
        merged_df["return_rate"] = merged_df.apply(
            lambda x: x["return_rate"]
            if np.abs(safe_div(x["diff"], x["net_value"])) < 0.0001
            else 1,
            axis=1,
        )
        merged_df["return_rate"] = (
            merged_df["return_rate"].replace(np.nan, 1).replace(0, 1).replace(np.inf, 1)
        )
        merged_df["cumulate_return_rate"] = merged_df["return_rate"].cumprod()

        merged_df.drop(
            columns=["lp_value_with_prev_liq", "suppose_net_value", "diff"],
            inplace=True,
        )

        merged_df.to_csv(
            os.path.join(config["path"], steps["s10"]["path"], address + ".csv")
        )
        pass
    except RuntimeWarning as e:
        print("warning address is ", address)
        raise e
    except Exception as e:
        print("error address is ", address)
        raise e


multi_thread = True

if __name__ == "__main__":
    args = sys.argv[1:]
    print("loading files")
    liq_file_list = os.listdir(steps["s9"]["path"])
    liq_file_list = filter(lambda e: e.endswith(".csv"), liq_file_list)
    liq_file_list = list(
        map(lambda e: os.path.join(steps["s9"]["path"], e), liq_file_list)
    )

    if len(args) > 0 and args[0] == "check_addr":
        print("check address in liqudiate folder, but not in cash folder")
        missing_addr_list = []
        for liq_file_path in liq_file_list:
            address = os.path.basename(liq_file_path).replace(".csv", "")
            cash_file_path = os.path.join(config["cash_path"], address + ".csv")
            if not os.path.exists(cash_file_path):
                print(address)
                missing_addr_list.append(address)
            # break
            pass
        print("address count:", len(missing_addr_list))
        exit(0)
    if len(args) > 0:
        addr_to_check = args[0]
    else:
        addr_to_check = ""
    print("reading prices")
    price_df = load_price().resample("1H").last().bfill()

    if multi_thread:
        with Pool(12) as p:
            res = list(
                tqdm(
                    p.imap(merge_return, liq_file_list),
                    ncols=120,
                    total=len(liq_file_list),
                )
            )
    else:
        with tqdm(total=len(liq_file_list), ncols=120) as pbar:
            for fpath in liq_file_list:
                merge_return(fpath)
                pbar.update()
