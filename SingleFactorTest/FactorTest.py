# -*- coding: utf-8 -*-
"""
Created at 2020/8/31 0031

@author:  Administrator 
"""
import os
import numpy as np
import pandas as pd
from Strategy.SingleFactorTest.DataProcess import *

FACTOR_DIR = r"E:\untitled\Strategy\SingleFactorTest\Factor"


class SingleFactorTest():
    def __init__(self, factors_name):
        self.__factors_name = factors_name
        self.__factor_dir = FACTOR_DIR
        self.__factors_dict = get_data(factors_name)
        self.__factors_name = factors_name
        self.__industry_mtx = pd.read_csv(r"E:\untitled\Strategy\Tushare Data\industry_dummy.csv", index_col=0)
        self.__mv_mtx = pd.read_csv(r'E:\untitled\Strategy\Tushare Data\temp\daily_basic\total_mv', index_col=0)

    def process_factors(self):
        """
        对原始因子进行清洗处理，包括筛选上市时间未满1年、ST股，去极值，标准化。
        :return:dict，清洗后的数据
        """
        origin_factors = self.__factors_dict
        st_series = is_st()
        st_series = st_series[st_series.is_st == 0].index
        last_date = 20200731
        year_dummy = is_one_year()
        oneyear = year_dummy.loc[last_date, :]
        stf_idx = st_series.intersection(oneyear[oneyear == 0].index)
        for f_name in self.__factors_name:
            temp_f = origin_factors.get(f_name)
            stf_idx = stf_idx.intersection(temp_f.columns)
            temp_f = temp_f[stf_idx]
            temp_f = temp_f.apply(lambda x: process_outlier(x), axis=0)
            temp_f = temp_f.apply(lambda x: z_score(x), axis=0)
            origin_factors[f_name] = temp_f
            print("Successfully processed factor value of", f_name)

        return origin_factors

    def neutralize(self):
        """
        对总市值进行取对处理，对行业哑变量进行转置
        :return: 行业哑变量和总市值对数矩阵
        """
        industry_df = self.__industry_mtx.T
        mv_df = self.__mv_mtx
        mv_df = np.log(mv_df)

        return industry_df, mv_df

    def single_factor_regression(self, f_name):
        industry_df, mv_df = self.neutralize()
        rt = daily_return()
        return 0

    def multi_results(self):
        for f_name in self.__factors_name:
            self.single_factor_regression()


if __name__ == '__main__':
    sft = SingleFactorTest(['pct_chg'])
    sft.process_factors()
