from decimal import Decimal

import pandas as pd
import math


def _x96_to_decimal(number: int):
    return Decimal(number) / 2 ** 96


def decimal_to_x96(number: Decimal):
    return int(Decimal(number) * 2 ** 96)



def tick_to_sqrt_price_x96(ticker) -> int:
    return int(decimal_to_x96(Decimal((Decimal.sqrt(Decimal(1.0001))) ** ticker)))


def tick_to_quote_price(tick: int, token_0_decimal, token_1_decimal, is_token0_base: bool):
    sqrt_price = tick_to_sqrt_price_x96(tick)
    decimal_price = _x96_to_decimal(sqrt_price) ** 2
    pool_price = decimal_price * Decimal(10 ** (token_0_decimal - token_1_decimal))
    return Decimal(1 / pool_price) if is_token0_base else pool_price


def fun(tick):
    if pd.isna(tick):
        return tick
    else:
        return float(tick_to_quote_price(tick, decimal0, decimal1, base0))

def flut(result, name):
    n = len(result) - 1
    result = result.drop(result.index[0])
    print(result)
    arr = result.loc[:,name].apply(lambda x: math.log(x+1))

    eu = (arr**2).sum()
    eu2 = (arr.sum()) ** 2
    print(n,eu,eu2,sep='\t')
    s = math.sqrt(eu/(n-1) - eu2/(n*(n-1)))
    fluctuation = s * math.sqrt(525600)
    return fluctuation

def fluthour(result, name):
    n = len(result)
    arr = result.loc[:,name].apply(lambda x: math.log(x+1))
    # print(arr)
    eu = (arr**2).sum()
    eu2 = (arr.sum()) ** 2
    # print(n,eu,eu2,sep='\t')
    s = math.sqrt(eu/(n-1) - eu2/(n*(n-1)))
    return s

# set info!!
decimal0 = 6  # USDC
decimal1 = 18
base0 = True  # tick is base on usdc?