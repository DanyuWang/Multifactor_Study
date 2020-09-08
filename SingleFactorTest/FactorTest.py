# -*- coding: utf-8 -*-
"""
Created at 2020/8/31 0031

@author:  Administrator 
"""
import os
import numpy as np
import pandas as pd
from scipy import stats
import statsmodels.api as sm
from Strategy.SingleFactorTest.DataProcess import *

FACTOR_DIR = r"E:\untitled\Strategy\SingleFactorTest\Factor"
SINGLE_RES_DIR = r"E:\untitled\Strategy\SingleFactorTest\SingleResults"


class SingleFactorTest():
    def __init__(self, factors_name):
        self._factors_name = factors_name
        self._factor_dir = r"E:\untitled\Strategy\SingleFactorTest\Factor"
        self._factors_dict = get_data(FACTOR_DIR, factors_name)
        self._factors_name = factors_name
        self._industry_mtx = pd.read_csv(r"E:\untitled\Strategy\Tushare Data\industry_dummy.csv", index_col=0)
        self._mv_mtx = pd.read_csv(r'E:\untitled\Strategy\Tushare Data\temp\daily_basic\total_mv', index_col=0)

    def process_factors(self):
        """
        对原始因子进行清洗处理，包括筛选上市时间未满1年、ST股，去极值，标准化。
        :return:dict，清洗后的数据
        """
        st_series = is_st()
        st_series = st_series[st_series.is_st == 0].index
        last_date = 20200731
        year_dummy = is_one_year()
        oneyear = year_dummy.loc[last_date, :]
        stf_idx = st_series.intersection(oneyear[oneyear == 0].index)
        for f_name in self._factors_name:
            temp_f = self._factors_dict.get(f_name)
            stf_idx = stf_idx.intersection(temp_f.columns)
            temp_f = temp_f[stf_idx]
            temp_f = temp_f.apply(lambda x: process_outlier(x), axis=0)
            temp_f = temp_f.apply(lambda x: z_score(x), axis=0)
            temp_f = temp_f[temp_f.isnull().sum(axis=1) / temp_f.shape[1] < 0.7]
            self._factors_dict[f_name] = temp_f
            print("Successfully processed factor value of", f_name)

        return 0

    def neutralize(self):
        """
        对总市值进行取对处理，对行业哑变量进行转置
        :return: 行业哑变量和总市值对数矩阵
        """
        industry_df = self._industry_mtx.T
        mv_df = self._mv_mtx
        mv_df = np.log(mv_df)

        return industry_df, mv_df

    @staticmethod
    def reshape(df_list, idx, cols):
        for i in range(len(df_list)):
            df_list[i] = df_list[i].loc[idx, cols]
        return df_list

    @staticmethod
    def WLS_regression(y, x_dict):
        X = np.column_stack((x_dict.get('industry').values, x_dict.get('mv').values, x_dict.get('factor').values))
        wls_model = sm.WLS(y.values, X, weights=x_dict.get('weight').values, missing='drop')
        results = wls_model.fit()

        t_value = results.tvalues[-1]
        f_param = results.params[-1]

        y_ic = wls_model.exog[:, -1]
        X_ic = wls_model.exog[:, :29]
        wls_ic = sm.WLS(y_ic, X_ic)
        ic_res = wls_ic.fit()
        spearman_corr = stats.spearmanr(ic_res.resid, wls_model.endog).correlation

        return t_value, f_param, spearman_corr

    def single_factor_regression(self, f_name):
        """
        对单个因子进行横截面回归
        :param f_name:因子名称
        :return:t检验的的t值序列，因子收益序列，IC序列;用于后续统计分析
        """
        industry_df, mv_df = self.neutralize()
        rt = daily_return()
        f_value = self._factors_dict.get(f_name)
        symbols = rt.columns.intersection(mv_df.columns).intersection(industry_df.index).intersection(f_value.columns)
        dates = rt.index.intersection(mv_df.index).intersection(f_value.index)
        f_value, mv_df, rt = self.reshape([f_value, mv_df, rt], dates, symbols)
        industry_df = industry_df.loc[symbols, :]

        tvs, fps, ics = [], [], []

        for i in range(len(dates) - 1):
            rt_s = rt.iloc[i+1, :]
            mv_s = mv_df.iloc[i, :]
            factor_s = f_value.iloc[i, :]
            x_dict = {'mv': mv_s, 'factor': factor_s, 'industry': industry_df, 'weight': np.sqrt(np.exp(mv_s))}
            t_value, f_param, ic_corr = self.WLS_regression(rt_s, x_dict)
            tvs.append(t_value)
            fps.append(f_param)
            ics.append(ic_corr)
            # print("FINISHED", i, 'date.')

        return tvs, fps, ics

    def multi_results(self):
        dict_list = []
        for f_name in self._factors_name:
            tvs, fps, ics = self.single_factor_regression(f_name)
            res_dict = {}
            res_dict['t_mean'] = np.mean(np.abs(tvs))
            res_dict['t_larger2'] = sum(np.abs(tvs) > 2) / len(tvs)
            res_dict['f_rt_mean'] = np.mean(fps)
            res_dict['f_rt_std'] = np.std(fps)
            res_dict['f_rt_zero'] = sum(np.array(fps) > 0) / len(fps)
            res_dict['IC_mean'] = np.mean(ics)
            res_dict['IC_std'] = np.std(ics)
            res_dict['IRIC'] = res_dict['IC_mean'] / res_dict['IC_std']
            res_dict['IC_larger0'] = sum(np.array(ics) > 0) / len(ics)

            dict_list.append(res_dict)
        res_df = pd.DataFrame(dict_list, index=self._factors_name)
        res_df.to_csv(os.path.join(SINGLE_RES_DIR, self._factors_name[0]+str(datetime.datetime.now())[:-7]+'.csv'))


if __name__ == '__main__':
    sft = SingleFactorTest(['RS_1'])
    sft.process_factors()
    tvs, fps, ics = sft.single_factor_regression('RS_1')
