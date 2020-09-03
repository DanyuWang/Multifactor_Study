import os
import time
import datetime
import pandas as pd
import tushare as ts

DATA_DIR = r"E:\untitled\Strategy\Tushare Data"


class TushareAPI():
    def __init__(self):
        self._api = ts.pro_api()
        self._data_dir = DATA_DIR
        self.__stock_info = pd.read_csv(os.path.join(self._data_dir, 'AShare_info.csv'))

    @staticmethod
    def export_csv(df, path):
        df.to_csv(path, index=False)

        return 0

    def collect_stock_basic(self):
        stocklist_P = self._api.stock_basic(exchange='', list_status='P',
                                            fields='ts_code,symbol,name,area,industry,market,list_status,list_date,'
                                                   'delist_date')
        stocklist_D = self._api.stock_basic(exchange='', list_status='D',
                                            fields='ts_code,symbol,name,area,industry,market,list_status,list_date,'
                                                   'delist_date')
        stocklist_L = self._api.stock_basic(exchange='', list_status='L',
                                            fields='ts_code,symbol,name,area,industry,market,list_status,list_date,'
                                                   'delist_date')
        stocklist_all = pd.concat([stocklist_L, stocklist_P, stocklist_D], sort=False)

        stocklist_all.to_csv(os.path.join(self._data_dir, 'AShare_info.csv'), index=False)

        return 0

    def collect_daily_history(self):
        info_df = self.__stock_info
        # all_codes = info_df['ts_codes']
        count = 0
        for code in info_df[info_df['delist_date'].isnull()]['ts_code']:
            if count % 500 == 0:
                time.sleep(60)
            if not os.path.exists(os.path.join(self._data_dir, 'daily', code)):
                temp_value = self._api.query('daily', ts_code=code, start_date='20100101')
                self.export_csv(temp_value, os.path.join(self._data_dir, 'daily', code))
                print(code, "daily history has been saved.")
                count += 1
            else:
                print(code, "daily history has been existed!")

        for idx, info in info_df[info_df['delist_date'].notnull()].iterrows():
            if info['delist_date'] < 20100101:
                print(info['ts_code'], 'quit before Year 2010.')
                continue
            if os.path.exists(os.path.join(self._data_dir, 'daily', info['ts_code'])):
                print(info['ts_code'], "daily history has been existed!")
                continue
            temp_value = self._api.query('daily', ts_code=info['ts_code'], start_date='20100101')
            self.export_csv(temp_value, os.path.join(self._data_dir, 'daily', info['ts_code']))
            print(info['ts_code'], "daily history has been saved.")

    def collect_monthly_history(self):
        info_df = self.__stock_info
        count = 0
        for code in info_df[info_df['delist_date'].isnull()]['ts_code']:
            if count % 500 == 0 and count > 0:
                time.sleep(60)
            if not os.path.exists(os.path.join(self._data_dir, 'monthly', code)):
                temp_value = self._api.query('monthly', ts_code=code, start_date='20100101')
                self.export_csv(temp_value, os.path.join(self._data_dir, 'monthly', code))
                print(code, "monthly history has been saved.")
                count += 1
            else:
                print(code, "monthly history has been existed!")

        for idx, info in info_df[info_df['delist_date'].notnull()].iterrows():
            if info['delist_date'] < 20100101:
                print(info['ts_code'], 'quit before Year 2010.')
                continue
            if os.path.exists(os.path.join(self._data_dir, 'monthly', info['ts_code'])):
                print(info['ts_code'], "monthly history has been existed!")
                continue
            temp_value = self._api.query('monthly', ts_code=info['ts_code'], start_date='20100101')
            self.export_csv(temp_value, os.path.join(self._data_dir, 'monthly', info['ts_code']))
            print(info['ts_code'], "monthly history has been saved.")

    def convert_daily_history(self):
        fields_files = {}  # 存放字段数据表
        daily_dir = os.path.join(self._data_dir, 'daily')
        for path, dir, files in os.walk(daily_dir):
            codes_list = files  # 股票代码列表

        first_df = pd.read_csv(os.path.join(daily_dir, codes_list[0]), index_col=['trade_date'])
        fields_name = first_df.columns[2:]
        for field in fields_name:
            fields_files[field] = first_df[field].rename(codes_list[0])
        count = 1
        for code in codes_list[1:]:
            temp_df = pd.read_csv(os.path.join(daily_dir, code), index_col=['trade_date'])
            for field in fields_name:
                fields_files[field] = pd.concat([fields_files[field], temp_df[field].rename(code)], axis=1)
            count += 1
            if count % 500 == 0:
                print("Have already covert", count, "stocks")

        for idx, v in fields_files.items():
            v.to_csv(os.path.join(self._data_dir, 'temp', idx))

    def collect_daily_basic(self):
        info_df = self.__stock_info
        # all_codes = info_df['ts_codes']
        count = 0
        for code in info_df[info_df['delist_date'].isnull()]['ts_code']:
            if count % 500 == 0:
                time.sleep(60)
            if not os.path.exists(os.path.join(self._data_dir, 'daily_basic', code)):
                temp_value = self._api.query('daily_basic', ts_code=code, start_date='20100101')
                self.export_csv(temp_value, os.path.join(self._data_dir, 'daily_basic', code))
                print(code, "daily basic has been saved.")
                count += 1
            else:
                print(code, "daily basic has been existed!")

        for idx, info in info_df[info_df['delist_date'].notnull()].iterrows():
            if info['delist_date'] < 20100101:
                print(info['ts_code'], 'quit before Year 2010.')
                continue
            if os.path.exists(os.path.join(self._data_dir, 'daily_basic', info['ts_code'])):
                print(info['ts_code'], "daily basic has been existed!")
                continue
            temp_value = self._api.query('daily_basic', ts_code=info['ts_code'], start_date='20100101')
            self.export_csv(temp_value, os.path.join(self._data_dir, 'daily_basic', info['ts_code']))
            print(info['ts_code'], "daily basic has been saved.")

    def convert_monthly_history(self):
        fields_files = {}  # 存放字段数据表
        monthly_dir = os.path.join(self._data_dir, 'monthly')
        for path, dir, files in os.walk(monthly_dir):
            codes_list = files  # 股票代码列表

        first_df = pd.read_csv(os.path.join(monthly_dir, codes_list[0]), index_col=['trade_date'])
        fields_name = first_df.columns[2:]
        for field in fields_name:
            fields_files[field] = first_df[field].rename(codes_list[0])
        count = 1
        for code in codes_list[1:]:
            temp_df = pd.read_csv(os.path.join(monthly_dir, code), index_col=['trade_date'])
            for field in fields_name:
                fields_files[field] = pd.concat([fields_files[field], temp_df[field].rename(code)], axis=1)
            count += 1
            if count % 500 == 0:
                print("Have already covert", count, "stocks")

        for idx, v in fields_files.items():
            v.to_csv(os.path.join(self._data_dir, 'temp', 'monthly',idx))

    def convert_daily_basic(self):
        fields_files = {}  # 存放字段数据表
        daily_dir = os.path.join(self._data_dir, 'daily_basic')
        for path, dir, files in os.walk(daily_dir):
            codes_list = files  # 股票代码列表

        first_df = pd.read_csv(os.path.join(daily_dir, codes_list[0]), index_col=['trade_date'])
        fields_name = first_df.columns[2:]
        for field in fields_name:
            fields_files[field] = first_df[field].rename(codes_list[0])
        count = 1
        for code in codes_list[1:]:
            temp_df = pd.read_csv(os.path.join(daily_dir, code), index_col=['trade_date'])
            for field in fields_name:
                fields_files[field] = pd.concat([fields_files[field], temp_df[field].rename(code)], axis=1)
            count += 1
            if count % 500 == 0:
                print("Have already covert", count, "stocks")

        for idx, v in fields_files.items():
            v.to_csv(os.path.join(self._data_dir, 'temp', 'daily_basic', idx))
        return 0


if __name__ == '__main__':
    tushare = TushareAPI()
    tushare.convert_daily_basic()
