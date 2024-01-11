from sqlalchemy import create_engine, Column, Integer, Float
from sqlalchemy.orm import declarative_base, sessionmaker
from weather_data import WeatherVariables
import weather_data as wd
import numpy as np
import pandas as pd


# Creates a base class for Weather to inherit
Base = declarative_base()


# C4
# Defines weather table
class Weather(Base):
    __tablename__ = "weather"
    weather_id = Column(Integer, primary_key=True)
    latitude = Column(Float)
    longitude = Column(Float)
    month = Column(Integer)
    day = Column(Integer)
    year = Column(Integer)
    avgTemp = Column(Float)
    minTemp = Column(Float)
    maxTemp = Column(Float)
    avgWindSpeed = Column(Float)
    minWindSpeed = Column(Float)
    maxWindSpeed = Column(Float)
    sumPrecip = Column(Float)
    minPrecip = Column(Float)
    maxPrecip = Column(Float)

    # Constructor for table variables
    def __init__(self, latitude, longitude, month, day, year, avgTemp, minTemp, maxTemp, avgWindSpeed, minWindSpeed,
                 maxWindSpeed, sumPrecip, minPrecip, maxPrecip):
        self.latitude = latitude
        self.longitude = longitude
        self.month = month
        self.day = day
        self.year = year
        self.avgTemp = avgTemp
        self.minTemp = minTemp
        self.maxTemp = maxTemp
        self.avgWindSpeed = avgWindSpeed
        self.minWindSpeed = minWindSpeed
        self.maxWindSpeed = maxWindSpeed
        self.sumPrecip = sumPrecip
        self.minPrecip = minPrecip
        self.maxPrecip = maxPrecip

# C6
    def show_table(self, name):
        # Prints formatted weather table using pandas dataframe
        table = pd.read_sql_table(table_name=name, con=engine)
        print(table)


# Connects to sqlite3 database and creates table
engine = create_engine('sqlite:///mydb.db')
Base.metadata.create_all(bind=engine)

# Creates session
Session = sessionmaker(bind=engine)
session = Session()

# C5
# Creates instance of WeatherVariables to populate variables with data from API
weather = WeatherVariables()
weather.avgTemp = (wd.year_data("2019-07-13")["temp_mean"][0] + wd.year_data("2020-07-13")["temp_mean"][0] +
                   wd.year_data("2021-07-13")["temp_mean"][0] + wd.year_data("2022-07-13")["temp_mean"][0] +
                   wd.year_data("2023-07-13")["temp_mean"][0]) / 5

weather.minTemp = min([wd.year_data("2019-07-13")["temp_min"][0], wd.year_data("2020-07-13")["temp_min"][0],
                       wd.year_data("2021-07-13")["temp_min"][0], wd.year_data("2022-07-13")["temp_min"][0],
                       wd.year_data("2023-07-13")["temp_min"][0]])

weather.maxTemp = max([wd.year_data("2019-07-13")["temp_max"][0], wd.year_data("2020-07-13")["temp_max"][0],
                       wd.year_data("2021-07-13")["temp_max"][0], wd.year_data("2022-07-13")["temp_max"][0],
                       wd.year_data("2023-07-13")["temp_max"][0]])

# Appends hourly wind data from API to lists to aggregate data
daily_wind2019 = []
daily_wind2020 = []
daily_wind2021 = []
daily_wind2022 = []
daily_wind2023 = []
for i in range(0, 23):
    avg_wind2019 = wd.hourly_data("2019-07-13")["hourly_wind_speed"][i]
    daily_wind2019.append(avg_wind2019)

    avg_wind2020 = wd.hourly_data("2020-07-13")["hourly_wind_speed"][i]
    daily_wind2020.append(avg_wind2020)

    avg_wind2021 = wd.hourly_data("2021-07-13")["hourly_wind_speed"][i]
    daily_wind2021.append(avg_wind2021)

    avg_wind2022 = wd.hourly_data("2022-07-13")["hourly_wind_speed"][i]
    daily_wind2022.append(avg_wind2022)

    avg_wind2023 = wd.hourly_data("2023-07-13")["hourly_wind_speed"][i]
    daily_wind2023.append(avg_wind2023)

weather.avgWindSpeed = (np.average(daily_wind2019) + np.average(daily_wind2020) + np.average(daily_wind2021) +
                        np.average(daily_wind2022) + np.average(daily_wind2023)) / 5

weather.minWindSpeed = min(daily_wind2019 + daily_wind2020 + daily_wind2021 + daily_wind2022 + daily_wind2023)

weather.maxWindSpeed = max(daily_wind2019 + daily_wind2020 + daily_wind2021 + daily_wind2022 + daily_wind2023)

weather.sumPrecip = (wd.year_data("2019-07-13")["precip_sum"][0] + wd.year_data("2020-07-13")["precip_sum"][0] +
                     wd.year_data("2021-07-13")["precip_sum"][0] + wd.year_data("2022-07-13")["precip_sum"][0] +
                     wd.year_data("2023-07-13")["precip_sum"][0])

weather.minPrecip = min([wd.year_data("2019-07-13")["precip_sum"][0], wd.year_data("2020-07-13")["precip_sum"][0],
                         wd.year_data("2021-07-13")["precip_sum"][0], wd.year_data("2022-07-13")["precip_sum"][0],
                         wd.year_data("2023-07-13")["precip_sum"][0]])

weather.maxPrecip = max([wd.year_data("2019-07-13")["precip_sum"][0], wd.year_data("2020-07-13")["precip_sum"][0],
                         wd.year_data("2021-07-13")["precip_sum"][0], wd.year_data("2022-07-13")["precip_sum"][0],
                         wd.year_data("2023-07-13")["precip_sum"][0]])

# Creates instance of Weather to populate weather table
weatherdata = Weather(weather.latitude, weather.longitude, weather.month, weather.day, weather.year, weather.avgTemp,
                      weather.minTemp, weather.maxTemp, weather.avgWindSpeed, weather.minWindSpeed,
                      weather.maxWindSpeed, weather.sumPrecip, weather.minPrecip, weather.maxPrecip)
# Inserts weather data to table and commits it
session.add(weatherdata)
session.commit()

# Uses instance to call show_table method to print formatted table
weatherdata.show_table('weather')
