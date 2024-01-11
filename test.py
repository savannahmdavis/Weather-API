import unittest
import pandas as pd
from weather_data import year_data, hourly_data
from weather_db import daily_wind2019


class WeatherTest(unittest.TestCase):

    # Tests if year_data() correctly returns a dataframe
    def test_dataframe(self):
        data = []
        df = pd.DataFrame(data)
        self.assertEqual(type(year_data("2023-07-13")), type(df))

    # Tests if dataframe returns a value
    def test_avg_temp(self):
        self.assertIsNotNone(year_data("2023-07-13")["temp_mean"][0])

    # Tests if values are appended to list
    def test_list(self):
        self.assertIn(hourly_data("2019-07-13")["hourly_wind_speed"][0], daily_wind2019)


if __name__ == "___main__":
    unittest.main()

