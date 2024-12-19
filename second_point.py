import httpx
from datetime import datetime
from dataset import data
from first_point import get_avg_data, time_counter
import numpy as np
import aiohttp
import asyncio

# second task
@time_counter
def get_sync_current_weather(KEY, url, CITY_NAME):
    params = {
        'q': CITY_NAME,
        'appid': KEY,
        'units': 'metric'
    }

    response = httpx.get(url, params=params)

    if response.status_code == 200:

        current_weather = response.json()
        current_weather = current_weather['main']['temp']
        return current_weather

    else:
        print(f"{response.status_code}")
        return None



# third, fourth tasks
def get_current_season():
    month = datetime.now().month
    if month in [3, 4, 5]:
        return "spring"
    elif month in [6, 7, 8]:
        return "summer"
    elif month in [9, 10, 11]:
        return "autumn"
    else:
        return "winter"


def check_current_weather():

    is_extreme = dict()
    for city in data['city'].unique():

        current_weather = get_sync_current_weather(KEY, url, city)

        if current_weather is not None:

            season = get_current_season()
            average_temp, average_std = get_avg_data(data)
            avg_temp = average_temp.loc[city, season]
            avg_std = average_std.loc[city, season]
            lower_bracket = avg_temp - 2 * avg_std
            high_bracket = avg_temp + 2 * avg_std

            if current_weather < lower_bracket or current_weather > high_bracket:
                is_extreme[city] = True

            else:
                is_extreme[city] = False

        else:
            is_extreme[city] = np.nan

    return is_extreme



#fifth task
async def get_async_current_weather(KEY, CITY_NAME):
    params = {
        'q': CITY_NAME,
        'appid': KEY,
        'units': 'metric'
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            if response.status == 200:
                current_weather = await response.json()
                temperature = current_weather['main']['temp']
                return temperature
            else:
                print(f"Error {response.status}: {await response.text()}")
                return None


@time_counter
async def main():
    is_extreme = dict()
    for city_name in data['city'].unique():
        current_weather = await get_async_current_weather(KEY=KEY, CITY_NAME=city_name)

        if current_weather is not None:

            season = get_current_season()
            average_temp, average_std = get_avg_data(data)
            avg_temp = average_temp.loc[city_name, season]
            avg_std = average_std.loc[city_name, season]
            lower_bracket = avg_temp - 2 * avg_std
            high_bracket = avg_temp + 2 * avg_std

            if current_weather < lower_bracket or current_weather > high_bracket:
                is_extreme[city_name] = True

            else:
                is_extreme[city_name] = False

        else:
            is_extreme[city_name] = np.nan

    return is_extreme


if __name__ == "__main__":
    #first task
    KEY = "b147affff78de7bc08eb45c49d209200"
    url = "http://api.openweathermap.org/data/2.5/weather"

    sync_is_extreme = check_current_weather()
    print(sync_is_extreme)


    result = asyncio.run(main())
    print(result)

### Асинхронка быстрее