import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry


# Function to pull API daily data and place in dataframe
def get_daily_data(start_date, end_date):
    """ (Hersbach et al., 2023; Muñoz Sabater, 2019; Schimanke et al., 2021; Zippenfenig, 2023)"""

    # Set up the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 49.2497,
        "longitude": -123.1193,
        "start_date": start_date,
        "end_date": end_date,
        "daily": ["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean", "precipitation_sum",
                  "wind_speed_10m_max"],
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch",
        "timezone": "America/Los_Angeles"
    }
    responses = openmeteo.weather_api(url, params=params)

    # Processes location with parameters as stated above
    response = responses[0]

    # Processes daily data into a dataframe
    daily = response.Daily()
    daily_temp_max = daily.Variables(0).ValuesAsNumpy()
    daily_temp_min = daily.Variables(1).ValuesAsNumpy()
    daily_temp_mean = daily.Variables(2).ValuesAsNumpy()
    daily_precip_sum = daily.Variables(3).ValuesAsNumpy()
    daily_wind_speed_max = daily.Variables(4).ValuesAsNumpy()

    daily_data = {"date": pd.date_range(
        start=pd.to_datetime(daily.Time(), unit="s"),
        end=pd.to_datetime(daily.TimeEnd(), unit="s"),
        freq=pd.Timedelta(seconds=daily.Interval()),
        inclusive="left"
    ), "temp_max": daily_temp_max, "temp_min": daily_temp_min,
        "temp_mean": daily_temp_mean, "precip_sum": daily_precip_sum,
        "wind_speed_max": daily_wind_speed_max}

    daily_df = pd.DataFrame(data=daily_data)

    return daily_df


# Function to pull API hourly data and place in dataframe
def get_hourly_data(start_date, end_date):
    """ (Hersbach et al., 2023; Muñoz Sabater, 2019; Schimanke et al., 2021; Zippenfenig, 2023)"""

    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 49.2497,
        "longitude": -123.1193,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "wind_speed_10m",
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch",
        "timezone": "America/Los_Angeles"
    }
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    # Processes hourly data (wind speed) into a dataframe
    hourly = response.Hourly()
    hourly_wind_speed = hourly.Variables(0).ValuesAsNumpy()

    hourly_wind_data = {"date": pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s"),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s"),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    ), "hourly_wind_speed": hourly_wind_speed}

    hourly_df = pd.DataFrame(data=hourly_wind_data)

    return hourly_df


# Calls get_daily_data to create a dataframe for a specific year, returns dataframe
def year_data(year_date):
    daily_data = get_daily_data(year_date, year_date)

    return daily_data


# Calls get_hourly_data to create a dataframe for a specific year
def hourly_data(year_date):
    wind_data = get_hourly_data(year_date, year_date)

    return wind_data


# C1
class WeatherVariables:
    # Creating constructor to initialize weather variables
    def __init__(self):
        self.latitude = 49.2497
        self.longitude = -123.1193
        self.month = 7
        self.day = 13
        self.year = 2023
        self.avgTemp = 0
        self.minTemp = 0
        self.maxTemp = 0
        self.avgWindSpeed = 0
        self.minWindSpeed = 0
        self.maxWindSpeed = 0
        self.sumPrecip = 0
        self.minPrecip = 0
        self.maxPrecip = 0

# C2
    # Pull average temperature in Fahrenheit for 2019-2023
    def get_avg_temp(self, date):
        weather_data = year_data(date)
        print("Mean temperature on {}: {:.1f} F".format(date, weather_data["temp_mean"][0]))

        return None

    # Pull maximum wind speed in miles per hour (mph) for 2019-2023
    def get_max_wind_speed(self, date):
        weather_data = year_data(date)
        print("Maximum wind speed on {}: {:.1f} mph".format(date, weather_data["wind_speed_max"][0]))

        return None

    # Pull precipitation sum in inches for 2019-2023
    def get_precip_sum(self, date):
        weather_data = year_data(date)
        print("Sum of precipitation on {}: {:.3f} inches".format(date, weather_data["precip_sum"][0]))

        return None
