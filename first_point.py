import pandas as pd
import numpy as np
from dataset import data
from datetime import datetime
from multiprocessing import Pool
from tqdm import tqdm
from loguru import logger

# first task
# не совсем понятно задание (нужно вычислять скользячку по каждому городу или в целом по всему датафрейму? если первый вариант, то он не оч информативный, поэтому посчитаю по каждому городу)
def get_rolling_mean(df):

    rolling_means = pd.DataFrame()

    for city in df['city'].unique():
        city_data = df.loc[df['city'] == city]
        city_data.set_index('timestamp', inplace=True)
        rolling_mean = city_data['temperature'].rolling(30).mean()
        rolling_mean.dropna(inplace=True)
        rolling_mean.name = city
        rolling_means = pd.concat([rolling_means, rolling_mean], axis=1)

    return rolling_means

# second task

def get_avg_data(data):
    average_temp = data.groupby(['city', 'season'])['temperature'].mean()
    average_std = data.groupby(['city', 'season'])['temperature'].std()
    return average_temp, average_std




def time_counter(func):
    def wrapper(*args, **kwargs):
        start = datetime.now()
        f = func(*args, **kwargs)
        end = datetime.now()
        logger.info(f"Длительность расчета функции {func.__name__} составила {(end-start).microseconds} в микросекундах")
        return f
    return wrapper


# third task
@time_counter
def get_outliers(df):
    outliers = pd.DataFrame()
    for city in df['city'].unique():
        for season in df['season'].unique():

            average_temp, average_std = get_avg_data(df)

            avg_temp = average_temp.loc[city, season]
            avg_std = average_std.loc[city, season]
            lower_bracket = avg_temp - 2 * avg_std
            high_bracket =  avg_temp + 2 * avg_std

            slice = df.loc[(df['city'] == city) & (df['season'] == season)]
            outlier = slice.loc[(slice['temperature'] < lower_bracket) | (slice['temperature'] > high_bracket)]
            outliers = pd.concat([outliers, outlier])

    return outliers


def get_city_oulier(args):
    df, city_name, season = args
    average_temp, average_std = get_avg_data(df)
    avg_temp_value = average_temp.loc[city_name, season]
    avg_std_value = average_std.loc[city_name, season]

    lower_bracket = avg_temp_value - 2 * avg_std_value
    high_bracket = avg_temp_value + 2 * avg_std_value

    slice_df = df[(df['city'] == city_name) & (df['season'] == season)]
    outlier = slice_df[(slice_df['temperature'] < lower_bracket) | (slice_df['temperature'] > high_bracket)]

    return (city_name, season, outlier)

@time_counter
def multi_process(df):
    args_list = [(df, city_name, season) for city_name in df['city'].unique() for season in df['season'].unique()]

    with Pool(10) as pool:
        results = pool.map(get_city_oulier, args_list)

    outliers = pd.DataFrame()
    for res in results:
        outliers = pd.concat([outliers, res[2]])

    return outliers

if __name__=="__main__":
    #1
    rolling_mean  = get_rolling_mean(data)

    #3
    outliers = get_outliers(data)
    print(outliers)

    # 4
    multi_processing = multi_process(data)
    print(multi_processing)



#### Распараллеливание процессоров не всегда приводит к более быстрым подсчетам


