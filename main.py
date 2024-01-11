from weather_data import WeatherVariables

print("-----Weather in Vancouver on July 13th from 2019-2023-----")
print("Enter 1 for average temperature in Fahrenheit")
print("Enter 2 for maximum wind speed in mph")
print("Enter 3 for precipitation sum in inches")
print("Enter 4 to exit")
user_input = input()

# List of dates for 7/13 spanning from 2019-2023
dates = ["2019-07-13", "2020-07-13", "2021-07-13", "2022-07-13", "2023-07-13"]

# Creates instance of class, WeatherVariables, from C1
vancouver_data = WeatherVariables()

while user_input != '4':
    if user_input == '1':
        # Gets average temperature on 7/13 for each year from 2019-2023
        for date in dates:
            vancouver_data.get_avg_temp(date)
    elif user_input == '2':
        # Gets maximum wind speed on 7/13 for each year 2019-2023
        for date in dates:
            vancouver_data.get_max_wind_speed(date)
    elif user_input == '3':
        # Gets sum of precipitation on 7/13 for each year 2019-2023
        for date in dates:
            vancouver_data.get_precip_sum(date)
    else:
        print("Your entry is invalid, please enter 1, 2, 3, or 4")
    user_input = input()


