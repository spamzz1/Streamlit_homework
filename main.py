import streamlit as st
from dataset import data
import pandas as pd
from first_point import get_avg_data
from second_point import get_sync_current_weather, get_current_season
import httpx
url = "http://api.openweathermap.org/data/2.5/weather"


st.set_page_config(
    layout="wide"
)
st.title("Here is an application")

uploaded_file = st.file_uploader("Choose a file", type=["csv"])

if uploaded_file is not None:
    try:
        data = pd.read_csv(uploaded_file)
        st.dataframe(data.head(5))



        selectbox = st.sidebar.selectbox(
            "Choose city to show historical weather",
            data['city'].unique()
            )


        average_temp, average_std = get_avg_data(data)
        st.text(f"Here is an average temperate in {selectbox}")
        st.dataframe(average_temp.loc[selectbox, :])
        st.text(f"Here is a std temperate in {selectbox}")
        st.dataframe(average_std.loc[selectbox, :])

        city_data = data.loc[data['city'] == selectbox][['timestamp', 'temperature']]
        city_data.set_index('timestamp', inplace=True)

        st.text(f"Here is a graph of {selectbox} temperature")
        st.line_chart(city_data, x_label="time", y_label="temperature")


    except Exception as e:
        st.error(f"Error reading the file: {e}")
else:
    st.text("No file has been chosen.")

api_key = st.text_input("Enter your API KEY")

if api_key:
    params = {
        'q': selectbox,
        'appid': api_key,
        'units': 'metric'
    }

    response = httpx.get(url, params=params)

    if response.status_code == 200:

        current_weather = response.json()
        current_weather = current_weather['main']['temp']

        season = get_current_season()
        average_temp, average_std = get_avg_data(data)
        avg_temp = average_temp.loc[selectbox, season]
        avg_std = average_std.loc[selectbox, season]
        lower_bracket = avg_temp - 2 * avg_std
        high_bracket = avg_temp + 2 * avg_std

        if current_weather < lower_bracket or current_weather > high_bracket:
            st.text(f"Current temperature in {selectbox} is {current_weather} is an outlier (extreme)")
        else:
            st.text(f"Current temperature in {selectbox} is {current_weather} within mean +-2 standart deviation")
    else:
        st.text({"cod": 401, "message": "Invalid API key. Please see https://openweathermap.org/faq#error401 for more info."})

