from datetime import date, datetime

from decimal import Decimal


steps = {
    "s1": {"path": "updated-txes"},
    "s2": {"path": "2_price.csv"},
    "s3": {"path": ""},
    "s4": {"path": "4_tick_fee", "map_file": "swap_tick_range.csv"},
    "s5": {
        "path": "5_position_liquidity.csv",
        "original_path": "5_position_liquidity.original.csv",
        "fixed_position_path": "5_fixed_position.csv",
    },
    "s6": {"path": "6_uni-nft-transfer.csv"},
    "s7": {
        "path": "7_position_address.csv",
        "original_path": "7_position_address.original.csv",
    },
    "s8": {
        "path": "8_position_fee",
        "mev_list": "8_mev_list.csv",
        "empty_log": "8_empty_log.tx",
    },
    "s9": {"path": "9_address_result"},
    "s10": {"path": "10_merge_with_cash"},
}
final_value_index = datetime(2038, 1, 1)


config_polygon = {
    "chain": "polygon",
    "demeter": "/data/src/demeter/demeter/__init__.py",
    "raw_path": "/data/uniswap/polygon/usdc-weth-005",
    "updated_path": "/data/uni-annual-review-2023/return_rate/polygon/updated-txes",
    "path": "/data/uni-annual-review-2023/return_rate/polygon/",
    "nft_transfer_file_path": "/data/uniswap/matic-proxy-transfer/nft-transfer-210505-231217.csv",
    "cash_path": "/data/research/uni-annual-review-2023/cash/polygon",
    "proxy_addr": "0xc36442b4a4522e871399cd717abdd847ab11fe88",
    "pool": {
        "name": "usdc-weth-005",
        "address": "0x45dda9cb7c25131df268515131f647d726f50608",
        "pool_fee_rate": Decimal(0.0005),
        "is_0_base": True,
        "decimal0": 6,
        "decimal1": 18,
    },
    "start": date(2023, 1, 1),
    "end": date(2023, 12, 17),
}


config = config_polygon
