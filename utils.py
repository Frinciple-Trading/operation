import datetime
import subprocess
import pandas as pd
import os
import json
import gzip
import shutil

PROD_MD_CSV_ROOT = "/mnt/prod/moneymaking/md_csv"
RES_MD_ROOT = "/mnt/research_data/md/"

METADATA_PATH = "/mnt/common_info/metadata.json"
VOLUME_RANK_DIR_ROOT = "/mnt/common_info/volume_rank/"

SPLIT_DATE = "2025-06-29"
RES_MD_COLUMNS = [
    "server_timestamp", "instrument", "exchange_timestamp",
    "acc_volume", "acc_turnover", "open_interest",
    "last_price", "last_volume", 
    "bid0_price", "bid1_price", "bid2_price", "bid3_price", "bid4_price",
    "bid0_qty",  "bid1_qty", "bid2_qty", "bid3_qty", "bid4_qty",
    "ask0_price", "ask1_price", "ask2_price", "ask3_price", "ask4_price",
    "ask0_qty",  "ask1_qty", "ask2_qty", "ask3_qty", "ask4_qty",
    "exchange_timestamp_str",
    "upper_limit_price", "lower_limit_price",
    "open_price", "close_price", "highest_price", "lowest_price"               
]

SHFE_FUTURE_NAME_PATTERN = r'^[a-z]{2}\d{4}$'
INE_FUTURE_NAME_PATTERN = r'^[a-z]{2}\d{4}$'
DCE_FUTURE_NAME_PATTERN = r'^[a-z]{1,2}\d{4}$'
ZCE_FUTURE_NAME_PATTERN = r'^[A-Z]{1,2}\d{3}$'
FUTURE_NAME_PATTERN_MAP = {
    "shfe" : SHFE_FUTURE_NAME_PATTERN,
    "ine" : INE_FUTURE_NAME_PATTERN,
    "dce" : DCE_FUTURE_NAME_PATTERN,
    "zce" : ZCE_FUTURE_NAME_PATTERN
}
COMMON_TRADING_HOURS = [
    ("20:58:59", "02:30:01"),
    ("08:58:59", "11:30:01"),
    ("13:28:59", "15:00:01")
]

def get_trading_days(start_date: str, end_date: str):
    # 转换成日期对象
    start = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
    # 所有工作日
    workdays_dt= []
    current = start
    while current <= end:
        if current.weekday() < 5:  # 0=周一, ..., 4=周五
            workdays_dt.append(current)
        current += datetime.timedelta(days=1)
    workdays = [d.strftime("%Y-%m-%d") for d in workdays_dt]
    # 删除假期
    with open("/mnt/common_info/holiday_calendar.json", "r", encoding="utf-8") as f:
        holiday_calendar = json.load(f)
        holidays = holiday_calendar["HOLIDAY"]
    trading_days = list(set(workdays) - set(holidays))
    trading_days.sort()
    return trading_days

def load_csv_from_gz(gz_file_path):
    # 1. 在 /mnt/ 下建立临时文件夹
    tmp_dir = f"/mnt/unzip_{gz_file_path.split('/')[-1]}"
    os.makedirs(tmp_dir, exist_ok=True)
    # 2. 解压 gz 文件到这个文件夹
    csv_file_path = os.path.join(tmp_dir, os.path.basename(gz_file_path).replace(".gz", ""))
    with gzip.open(gz_file_path, "rb") as f_in, open(csv_file_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    # 3. 读取 CSV
    df = pd.read_csv(csv_file_path)
    # 4. 删除临时文件夹（包括解压的文件）
    shutil.rmtree(tmp_dir)
    return df

def in_trading_hour(time):
    for (start, end) in COMMON_TRADING_HOURS:
        if end < start:
            if time > start or time < end:
                return True
        else:
            if time > start and time < end:
                return True
    return False

def get_volume_rank_dict(df):  # df[["InstrumentRoot", "InstrumentID", "Volume"]]
    # 1. 按 InstrumentRoot + InstrumentID 分组，取每个 InstrumentID 的最大 Volume
    df_max_volume = df.groupby(["InstrumentRoot", "InstrumentID"], as_index=False)["Volume"].max()
    # 2. 在每个 InstrumentRoot 内给 InstrumentID 排序，最大 Volume 排名 0
    df_max_volume["rank"] = df_max_volume.groupby("InstrumentRoot")["Volume"].rank(method="first", ascending=False).astype(int) - 1
    # 3. 构建嵌套字典
    result = {}
    for root, group in df_max_volume.groupby("InstrumentRoot"):
        result[root] = {}
        for _, row in group.iterrows():
            result[root][row["InstrumentID"]] = {
                "volume": int(row["Volume"]),
                "rank": int(row["rank"])
        }
    return result

def get_trading_hour_date_map(date):
    trading_hour_date_map = {}
    trading_hour_date_map[("08:00:00.000", "17:00:00.000")] = date
    
    date_dt = datetime.datetime.strptime(date, "%Y-%m-%d")
    weekday = date_dt.weekday()  # Monday=0, Sunday=6
    
    if weekday in [1,2,3,4]:  # Tue-Fri
        prev_date = (date_dt - datetime.timedelta(days=1)).date().strftime("%Y-%m-%d")
        trading_hour_date_map[("20:00:00.000", "24:00:00.000")] = prev_date
        trading_hour_date_map[("00:00:00.000", "03:00:00.000")] = date
    elif weekday == 0:
        last_friday = (date_dt - datetime.timedelta(days=3)).date().strftime("%Y-%m-%d")
        last_saturday = (date_dt - datetime.timedelta(days=2)).date().strftime("%Y-%m-%d")
        trading_hour_date_map[("20:00:00.000", "24:00:00.000")] = last_friday
        trading_hour_date_map[("00:00:00.000", "03:00:00.000")] = last_saturday
    
    return trading_hour_date_map

def build_exchange_ts_str(trading_hour_date_map, time):
    date = ""
    for trading_hour_session in trading_hour_date_map.keys():
        if time >= trading_hour_session[0] and time <= trading_hour_session[1]:
            date = trading_hour_date_map[trading_hour_session]
            break
    return date+" "+time