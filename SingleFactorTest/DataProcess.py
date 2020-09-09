# -*- coding: utf-8 -*-
"""
Created at 2020/8/27 0027

@author:  Administrator 
"""

import os
import datetime
import numpy as np
import pandas as pd
import Strategy.TushareAPI as ta

DATA_DIR = r"\Strategy\Tushare Data"
FACTOR_DIR = r"E:\untitled\Strategy\SingleFactorTest\Factor"
STOCK_INFO = pd.read_csv(r"E:\untitled\Strategy\Tushare Data\AShare_info.csv")


def get_data(data_dir, factor_names):
    """
    获取数据
    :param factor_names: list，提取字段列表
    :return: dict，和字段对应的数据表
    """
    data_dict = {}
    for factor_name in factor_names:
        print(os.path.join(data_dir, factor_name))
        data = pd.read_csv(os.path.join(data_dir, factor_name), index_col='trade_date')
        data_dict[factor_name] = data

    return data_dict


def is_st():
    """
    标记ST股
    :return: A股市场中所有股票是否为ST或者*ST股
    """
    return STOCK_INFO[['ts_code', 'is_st']].set_index('ts_code')


def is_one_year():
    """
    标记各个交易日上市不到一周年的股票
    :return: DataFrame，上市满365天则标记为1
    """
    cal_temp = ta.TushareAPI()._api.trade_cal(exchange='SSE', start_date='20100101')
    trade_day = cal_temp[cal_temp.is_open == 1]['cal_date']
    list_date = pd.to_datetime(STOCK_INFO['list_date'], format='%Y%m%d')
    cal_df = pd.concat([list_date] * len(trade_day), axis=1)

    cal_df = pd.DataFrame(np.where(
        cal_df.subtract(pd.to_datetime(trade_day.values, format='%Y%m%d'), axis=1) < datetime.timedelta(days=-365), 0, 1),
                          index=STOCK_INFO['ts_code'].values, columns=trade_day.values)
    cal_df = cal_df.T
    cal_df.index = cal_df.index.astype('int64')
    return cal_df


def process_outlier(x, k=5):
    """
    去极值
    :param x: 待处理序列
    :param k: 离散倍数
    :return:处理后的序列
    """
    med = np.median(x)
    mad = np.median(np.abs(x - med))
    up_limit = med + k * mad
    low_limit = med - k * mad

    processed = np.where(x >= up_limit, up_limit, np.where(x <= low_limit, low_limit, x))

    return processed


def z_score(x):
    """
    z-score标准化处理
    :param x:待标准化序列
    :return:处理后的序列
    """

    return (x - np.mean(x)) / np.std(x)


def daily_return():
    """
    计算个股日频收益率，第一行为空
    :return:收益率矩阵
    """
    close = pd.read_csv(r'E:\untitled\Strategy\Tushare Data\temp\close', index_col=0)
    temp_rt = close / close.shift() - 1

    return temp_rt
