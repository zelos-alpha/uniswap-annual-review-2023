import os
import pickle
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import NamedTuple, Tuple, Dict, List
import sys
import importlib.util
from config import config, steps

spec = importlib.util.spec_from_file_location("demeter", config["demeter"])
foo = importlib.util.module_from_spec(spec)
sys.modules["demeter"] = foo
spec.loader.exec_module(foo)

from demeter.uniswap.liquitidy_math import get_amounts

import pandas as pd


def get_value_in_base_token(amount0, amount1, price):
    if config["is_0_base"]:
        return amount0 + amount1 * price
    else:
        return amount0 * price + amount1


def get_value_in_base_token_with_decimal(amount0, amount1, price):
    if config["is_0_base"]:
        return (
            amount0 / 10 ** config["decimal0"]
            + amount1 / 10 ** config["decimal1"] * price
        )
    else:
        return (
            amount0 / 10 ** config["decimal0"] * price
            + amount1 / 10 ** config["decimal1"]
        )


def to_decimal(value) -> Decimal:
    return Decimal(value) if value else Decimal(0)


def to_int(value) -> int:
    return int(Decimal(value)) if value else 0


def format_date(ddd: date):
    return ddd.strftime("%Y-%m-%d")


def time2timestamp(t: datetime) -> int:
    return int(t.timestamp() * 1000)


def get_hour_time(t: datetime) -> datetime:
    return datetime(t.year, t.month, t.day, t.hour)


def get_minute_time(t: datetime) -> datetime:
    return datetime(t.year, t.month, t.day, t.hour, t.minute)


class PositionAction(Enum):
    MINT = "MINT"
    BURN = "BURN"
    COLLECT = "COLLECT"
    SWAP = "SWAP"


class PositionHistory(NamedTuple):
    timestamp: int
    blk_time: datetime
    fee0: Decimal
    fee1: Decimal
    liquidity: Decimal
    action: PositionAction


class Position(NamedTuple):
    id: str
    lower_tick: int
    upper_tick: int
    history: List[PositionHistory]
    addr_history: List[Tuple[int, int, str]]


@dataclass
class PositionLiquidity:
    id: str
    lower_tick: int
    upper_tick: int
    tx_type: str
    block_number: int
    tx_hash: str
    log_index: int
    blk_time: datetime
    liquidity: Decimal
    final_amount0: Decimal = Decimal(0)
    final_amount1: Decimal = Decimal(0)


@dataclass
class LivePosition:
    id: str
    lower_tick: int
    upper_tick: int
    liquidity: Decimal
    amount0: Decimal = Decimal(0)
    amount1: Decimal = Decimal(0)


class PositionManager(object):
    def __init__(self) -> None:
        self._content: Dict[str, Position] = {}

    def get_size_str(self) -> str:
        key_count = 0
        history_count = 0
        for v in self._content.values():
            key_count += 1
            history_count += len(v.history)

        return f"key count: {key_count}, value count: {history_count}"

    def add_position(self, id: str, lower: int, upper: int):
        if id not in self._content:
            position = Position(id, lower, upper, [], [])
            self._content[id] = position

    def add_history(
        self,
        id: str,
        blk_time: datetime,
        fee0: Decimal,
        fee1: Decimal,
        action: PositionAction,
        liquidity: Decimal = Decimal(0),
        address: str = None,
        fee_will_append=True,
    ):
        if id not in self._content:
            raise RuntimeError(f"{id} not found, call add_position first")
        pos = self._content[id]

        if not fee_will_append or len(pos.history) < 1:
            fee0 = fee0
            fee1 = fee1
        else:
            last_pos: PositionHistory = pos.history[len(pos.history) - 1]
            fee0 = fee0 + last_pos.fee0
            fee1 = fee1 + last_pos.fee1
        history = PositionHistory(
            time2timestamp(blk_time), blk_time, fee0, fee1, liquidity, action
        )
        pos.history.append(history)
        current_index = len(pos.history) - 1
        if address:
            last_idx = -1 if len(pos.addr_history) < 1 else len(pos.addr_history) - 1
            if last_idx > -1:
                pos.addr_history[last_idx] = current_index
            pos.addr_history.append((current_index, 999999999999, address))

    def dump_and_remove(self, save_path: str, key: str):
        file_name = os.path.join(save_path, key + ".pkl")
        with open(file_name, "wb") as f:
            pickle.dump(self._content[key], f)
        del self._content[key]


def get_pos_key(tx: pd.Series) -> str:
    if tx["position_id"] and not pd.isna(tx["position_id"]):
        return str(int(tx["position_id"]))
    return f'{tx["sender"]}-{int(tx["tick_lower"])}-{int(tx["tick_upper"])}'


def get_time_index(t: datetime, freq="1T") -> int:
    if freq == "1T":
        return t.hour * 60 + t.minute
    elif freq == "1H":
        return t.hour


def get_hour_end(t: datetime):
    return datetime(t.year, t.month, t.day, t.hour, 0) + timedelta(hours=1)


def get_hour_start(t: datetime):
    return datetime(t.year, t.month, t.day, t.hour, 0)


def limit_value(v, min_v, max_v):
    if v < min_v:
        return min_v
    if v > max_v:
        return max_v
    return v


def get_lp_net_value(
    liquidity: int, lower_tick: int, upper_tick: int, price, sqrt_price_x96
) -> Tuple[float, float, float]:
    amount0, amount1 = get_amounts(
        int(sqrt_price_x96),
        lower_tick,
        upper_tick,
        int(liquidity),
        config["decimal0"],
        config["decimal1"],
    )
    amount0 = float(amount0)
    amount1 = float(amount1)
    return (
        (amount0 + price * amount1)
        if config["is_0_base"]
        else (amount1 + price * amount0),
        amount0,
        amount1,
    )


def get_lp_values(
    liquidity: int, lower_tick: int, upper_tick: int, sqrt_price_x96
) -> (float, float):
    amount0, amount1 = get_amounts(
        sqrt_price_x96,
        lower_tick,
        upper_tick,
        liquidity,
        config["decimal0"],
        config["decimal1"],
    )
    return float(amount0), float(amount1)


def load_positions() -> pd.DataFrame:
    df = pd.read_csv(
        os.path.join(config["path"], steps["s5"]["path"]),
        dtype={"final_amount0": str, "final_amount1": str, "id": str, "liquidity": str},
        parse_dates=["blk_time"],
    )
    df["liquidity"] = df["liquidity"].apply(to_int)
    df["final_amount0"] = df["final_amount0"].apply(to_int)
    df["final_amount1"] = df["final_amount1"].apply(to_int)
    return df


def load_price() -> pd.DataFrame:
    prices_df = pd.read_csv(
        os.path.join(config["path"], steps["s2"]["path"]),
        converters={"sqrtPriceX96": str, "total_liquidity": str},
        parse_dates=["block_timestamp"],
    )
    prices_df = prices_df.set_index(["block_timestamp"])
    prices_df["sqrtPriceX96"] = prices_df["sqrtPriceX96"].apply(to_int)
    prices_df["total_liquidity"] = prices_df["total_liquidity"].apply(to_int)

    return prices_df


def load_daily_file(config, day_str) -> pd.DataFrame:
    file = os.path.join(
        config["updated_path"],
        f"{config['chain']}-{config['pool']['address']}-{day_str}.tick.csv",
    )

    df: pd.DataFrame = pd.read_csv(
        file,
        parse_dates=["block_timestamp"],
        converters={
            "amount0": to_decimal,
            "amount1": to_decimal,
            "total_liquidity": to_decimal,
            "liquidity": to_decimal,
        },
    )
    return df
