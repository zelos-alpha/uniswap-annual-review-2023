from multiprocessing import Pool

import os
import pandas as pd
import sys
from tqdm import tqdm

from config import steps, config


def check_one_file(fpath):
    df = pd.read_csv(os.path.join(config["path"], steps["s8"]["path"], fpath))
    if is_great:
        if len(df[df["cumprod_return_rate"] > value].index) > 0:
            # print(fpath)
            return fpath
    else:
        if len(df[df["cumprod_return_rate"] < value].index) > 0:
            # print(fpath)
            return fpath
    return ""


if __name__ == "__main__":
    file_list = os.listdir(os.path.join(config["path"], steps["s8"]["path"]))
    file_list = list(filter(lambda e: e.endswith(".csv"), file_list))
    args = sys.argv[1:]
    is_great = True if args[0] == "gt" else False
    value = float(args[1])
    res = []
    with Pool(8) as p:
        res = list(
            tqdm(
                p.imap(check_one_file, file_list),
                ncols=120,
                total=len(file_list),
            )
        )
    res = filter(lambda a: a != "", res)
    error_list = list(map(lambda a: a.replace(".csv", ""), res))
    print(error_list)
    print("有问题的文件个数: ", len(error_list))

    pass
