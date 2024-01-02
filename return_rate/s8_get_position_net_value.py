import math
import os.path
import sys
import time
import warnings
from datetime import timedelta, date, datetime
from multiprocessing import Pool
from typing import Tuple, Dict, List
from config import steps, config, final_value_index

import numpy as np
import pandas as pd
from tqdm import tqdm

from utils import (
    format_date,
    get_hour_time,
    get_value_in_base_token,
    get_minute_time,
    get_hour_end,
    get_hour_start,
    limit_value,
    get_lp_net_value,
    load_positions,
    load_price,
)

NET_VALUE_DECIMAL = 3

warnings.filterwarnings("error")


def load_fees(fee_type: str = "0", start=None, end=None) -> Dict[date, pd.DataFrame]:
    day = start
    day_fee_df = {}

    if env != ENV_PRD:
        this_pos = positions_df[positions_df["id"] == pos_id_to_test]
        end_day = this_pos.tail(1).iloc[0]["blk_time"].date()
        if this_pos["liquidity"].sum() > 100:
            end_day = global_end
        to_load_day = pd.date_range(
            this_pos.head(1).iloc[0]["blk_time"].date(), end_day, freq="1D"
        )
    with tqdm(total=(end - start).days + 1, ncols=150) as pbar:
        while day <= end:
            if (
                    env != ENV_PRD
                    and datetime(day.year, day.month, day.day) not in to_load_day
            ):
                day += timedelta(days=1)
                pbar.update()
                continue

            day_str = format_date(day)
            fee_df = pd.read_csv(
                os.path.join(
                    config["path"], steps["s4"]["path"], f"{day_str}_{fee_type}.csv"
                ),
                index_col=0,
                parse_dates=True,
            )

            day_fee_df[day] = fee_df
            day += timedelta(days=1)
            pbar.update()
    return day_fee_df


def get_sum_fee_df(
        calc_start: datetime,
        calc_end: datetime,
        lower_tick: int,
        upper_tick: int,
        hourly: bool,
) -> (pd.Series, pd.Series):

    if calc_start == calc_end:
        return pd.Series(), pd.Series()

    start_date = calc_start.date()
    end_date = (calc_end - timedelta(microseconds=1)).date()
    day = start_date
    sum0 = pd.Series()
    sum1 = pd.Series()
    if hourly:
        day_fee_df_0, day_fee_df_1 = (day_fee_df_hour_0, day_fee_df_hour_1)
    else:
        day_fee_df_0, day_fee_df_1 = (day_fee_df_minute_0, day_fee_df_minute_1)

    while day <= end_date:
        day_start_time = day_fee_df_0[day].index[0]
        day_end_time = day_fee_df_0[day].tail(1).index[0] + (
            timedelta(hours=1) if hourly else timedelta(minutes=1)
        )
        if day == start_date:
            day_start_time = calc_start
        if day == end_date:
            day_end_time = calc_end

        day_min_tick = tick_range_df.loc[day]["min_tick"]
        day_max_tick = tick_range_df.loc[day]["max_tick"]
        day_fee_0 = day_fee_df_0[day]
        day_fee_1 = day_fee_df_1[day]

        if lower_tick > day_max_tick or upper_tick < day_min_tick:
            sum0 = pd.concat(
                [
                    sum0,
                    pd.Series(
                        0,
                        index=day_fee_0[
                            (day_fee_0.index >= day_start_time)
                            & (day_fee_0.index < day_end_time)
                            ].index,
                    ),
                ]
            )
            sum1 = pd.concat(
                [
                    sum1,
                    pd.Series(
                        0,
                        index=day_fee_1[
                            (day_fee_1.index >= day_start_time)
                            & (day_fee_1.index < day_end_time)
                            ].index,
                    ),
                ]
            )
            day += timedelta(days=1)
            continue
        real_lower_tick = limit_value(lower_tick, day_min_tick, day_max_tick)
        real_upper_tick = limit_value(upper_tick, day_min_tick, day_max_tick)
        lower_index = int(real_lower_tick - day_min_tick)
        upper_index = int(real_upper_tick - day_min_tick) + 1
        sum0 = pd.concat(
            [
                sum0,
                day_fee_0[
                    (day_fee_0.index >= day_start_time)
                    & (day_fee_0.index < day_end_time)
                    ]
                .iloc[:, lower_index:upper_index]
                .sum(axis=1),
            ]
        )
        sum1 = pd.concat(
            [
                sum1,
                day_fee_1[
                    (day_fee_1.index >= day_start_time)
                    & (day_fee_1.index < day_end_time)
                    ]
                .iloc[:, lower_index:upper_index]
                .sum(axis=1),
            ]
        )
        day += timedelta(days=1)

    return sum0, sum1


def calc_value_between(
        begin_time, end_time, lower_tick, upper_tick, current_liquidity, hourly
):
    # def find_liq_in_price(idx):
    #     return prices_df.loc[idx]["total_liquidity"]
    def calc_net_value(row) -> Tuple[float, float, float]:
        return get_lp_net_value(
            current_liquidity, lower_tick, upper_tick, row["price"], row["sqrtPriceX96"]
        )

    if begin_time >= end_time:
        return pd.DataFrame()
    pool_calc_fee0, pool_calc_fee1 = get_sum_fee_df(
        begin_time, end_time, lower_tick, upper_tick, hourly
    )
    df = pd.DataFrame(
        {"pool_calc_fee0": pool_calc_fee0, "pool_calc_fee1": pool_calc_fee1}
    )

    df["total_liquidity"] = prices_df.loc[
                            begin_time:end_time, "total_liquidity"
                            ]  # df.index.map(find_liq_in_price)
    df["calc_fee0"] = current_liquidity / df["total_liquidity"] * df["pool_calc_fee0"]
    df["calc_fee1"] = current_liquidity / df["total_liquidity"] * df["pool_calc_fee1"]
    if hourly:
        df["price"] = prices_hourly_df.loc[begin_time:end_time, "price"]
        df["sqrtPriceX96"] = prices_hourly_df.loc[begin_time:end_time, "sqrtPriceX96"]
    else:
        df["price"] = prices_df.loc[begin_time:end_time, "price"]
        df["sqrtPriceX96"] = prices_df.loc[begin_time:end_time, "sqrtPriceX96"]
    df["liquidity"] = current_liquidity
    df[["lp_value", "amount0", "amount1"]] = df.apply(
        calc_net_value,
        axis=1,
        result_type="expand",
    )
    return df


def filter_mev_operation(actions: pd.DataFrame) -> List[int]:

    if len(actions.index) < 1:
        return []
    actions["liq_sum"] = actions["liquidity"].cumsum()
    total_list = []
    local_list = []
    liq_start = None
    idx = 0
    for index, action in actions.iterrows():
        if liq_start is None:
            liq_start = action["blk_time"]

        local_list.append(index)
        if (
                action["tx_type"] == "COLLECT" and action["liq_sum"] == 0
        ):  # end of a position
            if idx > 0 and action["tx_hash"] != actions.iloc[idx - 1]["tx_hash"]:
                if (action["blk_time"] - liq_start) < timedelta(minutes=1):
                    total_list.extend(local_list)
                    local_list = []
                liq_start = None
        idx += 1
    return total_list


def write_empty_log(position_id):
    with open(os.path.join(config["path"], steps["s8"]["empty_log"]), "a") as p:
        p.write(f"{position_id}")
        p.write("\n")


def merge_actions_in_same_minute(actions: pd.DataFrame):
    actions["key"] = actions.apply(
        lambda x: str(int(x["minute_time"].timestamp())) + "_" + x["tx_type"], axis=1
    )
    actions = actions.drop(columns=["tx_hash"])
    merged_actions = actions.groupby("key").aggregate(
        {
            "id": "last",
            "lower_tick": "last",
            "upper_tick": "last",
            "tx_type": "last",
            "block_number": "last",
            "minute_time": "last",
            "log_index": "last",
            "blk_time": "first",
            "liquidity": "sum",
            "final_amount0": "sum",
            "final_amount1": "sum",
        }
    )

    def get_order(tp):
        match (tp):
            case "MINT":
                return 1
            case "BURN":
                return 2
            case "COLLECT":
                return 3

    merged_actions["order"] = merged_actions["tx_type"].apply(get_order)
    return merged_actions.sort_values(["minute_time", "order"])


def process_one_position(param: Tuple[str, pd.DataFrame]):
    position_id, rel_actions = param
    if position_id in ["5"]:
        return

    MIN_LIQUIDATION = 100

    try:

        sum_liq = rel_actions["liquidity"].sum()
        if sum_liq > MIN_LIQUIDATION:
            rel_actions.loc[999999999999999] = {
                "tx_type": "END",
                "blk_time": datetime.combine(global_end, datetime.min.time())
                            + timedelta(days=1)
                            - timedelta(minutes=1),
            }
        elif sum_liq < 0:
            raise RuntimeError(
                f"position {position_id} doesn't have a mint, sum liq is {sum_liq}"
            )

        # remove empty collect and burn
        useful_actions = rel_actions.drop(
            rel_actions.loc[
                (rel_actions["tx_type"] == "COLLECT")
                & (rel_actions["final_amount0"] == 0)
                & (rel_actions["final_amount1"] == 0)
                ].index
        )
        useful_actions = useful_actions.drop(
            rel_actions.loc[
                (rel_actions["tx_type"] == "BURN")
                & (rel_actions["liquidity"] == 0)
                & (rel_actions["final_amount0"] == 0)
                & (rel_actions["final_amount1"] == 0)
                ].index
        )
        if len(useful_actions.index) <= 0:
            write_empty_log(position_id)
            return
        if len(useful_actions.index) > 1:
            last_timestamp = useful_actions.tail(1).iloc[0]["blk_time"]
            useful_actions.loc[
                (useful_actions[useful_actions["blk_time"] == last_timestamp]).index,
                "blk_time",
            ] = last_timestamp + timedelta(minutes=1)
        useful_actions["minute_time"] = useful_actions.apply(
            lambda x: get_minute_time(x["blk_time"]), axis=1
        )
        mev_list = filter_mev_operation(useful_actions)
        if len(mev_list) > 0:
            with open(os.path.join(config["path"], steps["s8"]["mev_list"]), "a") as f:
                f.write(position_id + "\n")
            useful_actions = useful_actions.drop(index=mev_list)
        if len(useful_actions.index) <= 0:
            write_empty_log(position_id)
            return
        useful_actions = merge_actions_in_same_minute(useful_actions)
        useful_actions = useful_actions.sort_values(
            ["block_number", "log_index"]
        )

        upper_tick = useful_actions.iloc[0]["upper_tick"]
        lower_tick = useful_actions.iloc[0]["lower_tick"]
        current_liquidity = 0

        previous_action_time = useful_actions.head(1).iloc[0]["minute_time"]
        pos_collect_value_df = pd.DataFrame()
        pos_value_df = pd.DataFrame()
        pending_0 = pending_1 = 0.0
        # last_index = useful_actions.tail(1).index[0]
        for index, action_row in useful_actions.iterrows():
            action_start = previous_action_time
            is_last_minute = True
            if action_start < action_row["minute_time"]:
                action_end = action_row[
                    "minute_time"
                ]  # if index != last_index else action_row["minute_time"] + timedelta(minutes=1)
                if get_hour_time(action_start) == get_hour_time(action_end):
                    pos_collect_value_df = pd.concat(
                        [
                            pos_collect_value_df,
                            calc_value_between(
                                action_start,
                                action_end,
                                lower_tick,
                                upper_tick,
                                current_liquidity,
                                False,
                            ),
                        ]
                    )
                else:
                    start_hour_end = get_hour_end(action_start)
                    end_hour_begin = get_hour_start(action_end)
                    pos_collect_value_df = pd.concat(
                        [
                            pos_collect_value_df,
                            calc_value_between(
                                action_start,
                                start_hour_end,
                                lower_tick,
                                upper_tick,
                                current_liquidity,
                                False,
                            ),
                        ]
                    )
                    pos_collect_value_df = pd.concat(
                        [
                            pos_collect_value_df,
                            calc_value_between(
                                start_hour_end,
                                end_hour_begin,
                                lower_tick,
                                upper_tick,
                                current_liquidity,
                                True,
                            ),
                        ]
                    )
                    pos_collect_value_df = pd.concat(
                        [
                            pos_collect_value_df,
                            calc_value_between(
                                end_hour_begin,
                                action_end,
                                lower_tick,
                                upper_tick,
                                current_liquidity,
                                False,
                            ),
                        ]
                    )
                    if end_hour_begin + timedelta(minutes=1) >= action_end:
                        is_last_minute = False

            match action_row["tx_type"]:
                case "MINT":
                    current_liquidity += action_row["liquidity"]
                    pass
                case "BURN":
                    current_liquidity += action_row["liquidity"]

                    pending_0 += action_row.final_amount0 / 10 ** config["decimal0"]
                    pending_1 += action_row.final_amount1 / 10 ** config["decimal1"]
                    pass
                case "COLLECT":
                    actual_fee0 = (
                            action_row.final_amount0 / 10 ** config["decimal0"] - pending_0
                    )  # real_fee0
                    actual_fee1 = (
                            action_row.final_amount1 / 10 ** config["decimal1"] - pending_1
                    )  # real_fee0

                    if len(pos_collect_value_df.index) >= 1:
                        pos_collect_value_df["tvalue"] = pos_collect_value_df.index
                        pos_collect_value_df["time_delta_second"] = (
                            (
                                    pos_collect_value_df["tvalue"].shift(-1)
                                    - pos_collect_value_df["tvalue"]
                            )
                            .fillna(
                                timedelta(minutes=1)
                                if is_last_minute
                                else timedelta(hours=1)
                            )
                            .apply(lambda x: x.total_seconds())
                        )
                        pos_collect_value_df["fee_modify_rate"] = (
                                pos_collect_value_df["time_delta_second"]
                                / 60
                                * pos_collect_value_df["liquidity"]
                        )
                        total_fee_time_liq = pos_collect_value_df[
                            "fee_modify_rate"
                        ].sum()
                        pos_collect_value_df["fee_modify_rate"] = (
                                pos_collect_value_df["fee_modify_rate"] / total_fee_time_liq
                        )
                        pos_collect_value_df["fee_modify_rate"] = pos_collect_value_df[
                            "fee_modify_rate"
                        ].fillna(0)

                        if len(pos_collect_value_df.index) > 0:
                            calc_fee0 = pos_collect_value_df["calc_fee0"].sum()
                            calc_fee1 = pos_collect_value_df["calc_fee1"].sum()
                        else:
                            calc_fee0 = 0
                            calc_fee1 = 0
                        fee_diff0 = actual_fee0 - calc_fee0
                        fee_diff1 = actual_fee1 - calc_fee1
                        pos_collect_value_df["fee_modify0"] = (
                                pos_collect_value_df["fee_modify_rate"] * fee_diff0
                        )
                        pos_collect_value_df["fee_modify1"] = (
                                pos_collect_value_df["fee_modify_rate"] * fee_diff1
                        )
                        pos_collect_value_df["fee0"] = (
                                pos_collect_value_df["calc_fee0"]
                                + pos_collect_value_df["fee_modify0"]
                        )
                        pos_collect_value_df["fee1"] = (
                                pos_collect_value_df["calc_fee1"]
                                + pos_collect_value_df["fee_modify1"]
                        )
                        pos_collect_value_df["cumsum_fee0"] = pos_collect_value_df[
                            "fee0"
                        ].cumsum()
                        pos_collect_value_df["cumsum_fee1"] = pos_collect_value_df[
                            "fee1"
                        ].cumsum()

                        pos_value_df = pd.concat([pos_value_df, pos_collect_value_df])
                    pending_0 = pending_1 = 0
                    pos_collect_value_df = pd.DataFrame()
                    pass
                case "END":
                    if len(pos_collect_value_df.index) < 1:
                        return
                    pos_collect_value_df["tvalue"] = pos_collect_value_df.index
                    pos_collect_value_df["time_delta_second"] = (
                        (
                                pos_collect_value_df["tvalue"].shift(-1)
                                - pos_collect_value_df["tvalue"]
                        )
                        .fillna(
                            timedelta(minutes=1)
                            if is_last_minute
                            else timedelta(hours=1)
                        )
                        .apply(lambda x: x.total_seconds())
                    )
                    pos_collect_value_df["fee_modify_rate"] = 0
                    pos_collect_value_df["fee_modify0"] = 0
                    pos_collect_value_df["fee_modify1"] = 0
                    pos_collect_value_df["fee0"] = pos_collect_value_df["calc_fee0"]
                    pos_collect_value_df["fee1"] = pos_collect_value_df["calc_fee1"]
                    pos_collect_value_df["cumsum_fee0"] = pos_collect_value_df[
                        "fee0"
                    ].cumsum()
                    pos_collect_value_df["cumsum_fee1"] = pos_collect_value_df[
                        "fee1"
                    ].cumsum()
                    pos_value_df = pd.concat([pos_value_df, pos_collect_value_df])
            previous_action_time = useful_actions.loc[index]["minute_time"]
            pass

        def get_total_nv(row):
            fee_nv = get_value_in_base_token(
                row["cumsum_fee0"], row["cumsum_fee1"], row["price"]
            )
            return fee_nv + row["lp_value"]

        if (
                len(pos_value_df.index) < 1
        ):
            with open(os.path.join(config["path"], steps["s8"]["path"]), "a") as f:
                f.write(position_id + "\n")
            return
        pos_value_df["total_net_value"] = pos_value_df.apply(get_total_nv, axis=1)
        total_net_value_prev = pos_value_df["total_net_value"].shift(1).bfill()
        pos_value_df["return_rate"] = pos_value_df["total_net_value"].round(
            NET_VALUE_DECIMAL
        ) / total_net_value_prev.round(NET_VALUE_DECIMAL)
        pos_value_df.loc[
            pos_value_df.index.isin(useful_actions["minute_time"].values), "return_rate"
        ] = 1
        useful_actions["blk_time"]
        pos_value_df["return_rate"] = (
            pos_value_df["return_rate"]
            .fillna(1)  # no net value
            .replace(np.inf, 1)  # pre value is 0
            .replace(-np.inf, 1)  # pre value is 0,
            .replace(0, 1)  # net value is 0
        )

        if pos_value_df.tail(1).index - pos_value_df.head(1).index < timedelta(hours=2):
            is_pos_value_df_minute = True
        else:
            pos_value_df = pos_value_df.resample("1H").agg(
                {
                    "pool_calc_fee0": "sum",
                    "pool_calc_fee1": "sum",
                    "calc_fee0": "sum",
                    "calc_fee1": "sum",
                    "liquidity": "last",
                    "total_liquidity": "last",
                    "price": "last",
                    "amount0": "last",
                    "amount1": "last",
                    "sqrtPriceX96": "last",
                    "lp_value": "last",
                    # "fee_modify_rate": "last",
                    "fee_modify0": "sum",
                    "fee_modify1": "sum",
                    "fee0": "sum",
                    "fee1": "sum",
                    "cumsum_fee0": "last",
                    "cumsum_fee1": "last",
                    "time_delta_second": "sum",
                    "total_net_value": "last",
                    "return_rate": "prod",
                }
            )
            is_pos_value_df_minute = False

        pos_value_df["cumprod_return_rate"] = pos_value_df["return_rate"].cumprod()
        pos_value_df = pos_value_df.drop(
            columns=[
                # "price_prev",
                # "sqrtPriceX96_prev",
                "calc_fee0",
                "calc_fee1",
                # "net_value_with_pre_price",
                # "cumsum_fee0_prev",
                # "cumsum_fee1_prev",
            ]
        )
        if is_pos_value_df_minute:
            pos_value_df = pos_value_df.resample("1H").agg(
                {
                    "pool_calc_fee0": "sum",
                    "pool_calc_fee1": "sum",
                    "liquidity": "last",
                    "total_liquidity": "last",
                    "price": "last",
                    "amount0": "last",
                    "amount1": "last",
                    "lp_value": "last",
                    "fee_modify0": "sum",
                    "fee_modify1": "sum",
                    "fee0": "sum",
                    "fee1": "sum",
                    "cumsum_fee0": "last",
                    "cumsum_fee1": "last",
                    "time_delta_second": "sum",
                    "total_net_value": "last",
                    "return_rate": "prod",
                    "cumprod_return_rate": "last",
                    "sqrtPriceX96": "last",
                }
            )

        def get_lp_value_with_prev_liq(row: pd.Series) -> float:
            pass
            prev_liq = (
                row["liquidity_prev"]
                if not (
                        row["liquidity_prev"] is None or math.isnan(row["liquidity_prev"])
                )
                else 0
            )
            lp_val, amt0, amt1 = get_lp_net_value(
                prev_liq, lower_tick, upper_tick, row["price"], row["sqrtPriceX96"]
            )
            return lp_val

        pos_value_df["liquidity_prev"] = pos_value_df["liquidity"].shift(1).fillna(0)
        pos_value_df["lp_value_with_prev_liq"] = pos_value_df.apply(
            get_lp_value_with_prev_liq, axis=1
        )
        pos_value_df.drop(
            columns=[
                "liquidity_prev",
                "sqrtPriceX96",
            ],
            inplace=True,
        )
        final_value_row = pos_value_df.tail(1).copy()
        if sum_liq <= MIN_LIQUIDATION:
            pos_value_df.loc[
                final_value_row.index[0],
                [
                    "liquidity",
                    "lp_value",
                    "total_net_value",
                    "amount0",
                    "amount1",
                    # "lp_value_with_prev_liq",
                ],
            ] = 0
        pos_value_df.loc[final_value_index] = final_value_row.iloc[0]
        # save file
        pos_value_df.to_csv(
            os.path.join(config["path"], steps["s8"]["path"], f"{position_id}.csv")
        )
    except RuntimeWarning as e:
        print("warning pos is ", position_id, index)
        raise e
    except Exception as e:
        print("error pos is ", position_id, index)
        raise e

    pass


freq = "1H"
freq_short = "1T"
ENV_PRD = "prd"


pos_id_to_test = "843340"


if __name__ == "__main__":
    if len(sys.argv) > 1:
        env = sys.argv[1]  # dev or prd
    else:
        env = "dev"
    start_time = time.time()
    global_start = config["start"]
    global_end = config["end"]
    if os.path.exists(os.path.join(config["path"], steps["s8"]["mev_list"])):
        os.remove(os.path.join(config["path"], steps["s8"]["mev_list"]))

    print("loading position_liquidity")

    positions_df = load_positions()
    print("loading fees")

    day_fee_df_minute_0 = load_fees("0", global_start, global_end)
    day_fee_df_hour_0 = {
        k: v.resample(freq).sum() for k, v in day_fee_df_minute_0.items()
    }

    day_fee_df_minute_1 = load_fees("1", global_start, global_end)
    day_fee_df_hour_1 = {
        k: v.resample(freq).sum() for k, v in day_fee_df_minute_1.items()
    }

    tick_range_df = pd.read_csv(
        os.path.join(config["path"], steps["s4"]["path"], steps["s4"]["map_file"]),
        parse_dates=["day_str"],
    )
    tick_range_df["day_str"] = tick_range_df["day_str"].apply(
        lambda a: a.date()
    )
    tick_range_df = tick_range_df.set_index(["day_str"])

    print("loading price")
    prices_df = load_price()
    prices_df = prices_df.resample(freq_short).last().bfill()
    pool_begin_index = pd.date_range(
        get_minute_time(positions_df["blk_time"].min()),
        get_minute_time(prices_df.head(1).index[0] - timedelta(minutes=1)),
        freq=freq_short,
    )
    pool_begin_df = pd.DataFrame(index=pool_begin_index)
    prices_df = pd.concat([pool_begin_df, prices_df]).bfill()
    prices_hourly_df = prices_df.resample(freq).last().bfill()
    print("start calc")
    ids = positions_df.groupby("id")
    current_time = datetime.combine(
        global_end + timedelta(days=1), datetime.min.time()
    ) - timedelta(minutes=1)
    if env == ENV_PRD:
        ids = list(ids)
        with Pool(28) as p:
            res = list(
                tqdm(p.imap(process_one_position, ids), ncols=120, total=len(ids))
            )
    else:
        with tqdm(total=len(ids), ncols=150) as pbar:
            for position_id, rel_actions in ids:
                if position_id != pos_id_to_test:
                    pbar.update()
                    continue

                process_one_position((str(position_id), rel_actions))
                pbar.update()
    print("exec time", time.time() - start_time)
