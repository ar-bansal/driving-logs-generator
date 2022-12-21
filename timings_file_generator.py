import pandas as pd
import holidays
from datetime import datetime, timedelta, time
from randomtimestamp import random_time
import sys
import random
import numpy as np
import calendar


def validate_date(input_date):
    check = input_date.split("-")
    c1 = len(check) == 3
    try:
        datetime(year=int(check[2]), month=int(check[1]), day=int(check[0]))
        c2 = True
    except:
        c2 = False
    
    conditions = [c1, c2]

    return all(conditions)


def validate_time(input_time):
    check = input_time.split(":")
    c1 = len(check) == 3
    try:
        time(hour=int(check[0]), minute=int(check[1]), second=int(check[2]))
        c2 = True
    except:
        c2 = False
    
    conditions = [c1, c2]

    return all(conditions)


def validate_df(df):
    try:
        pd.read_csv(df)
        check = True
    except:
        check = False
    return check


def validate_inputs(start_date, end_date, onward_start_time, onward_end_time, return_start_time, return_end_time, addresses):
    ost = onward_start_time.split(":")
    ost = time(hour=int(ost[0]), minute=int(ost[1]), second=int(ost[2]))
    oet = onward_end_time.split(":")
    oet = time(hour=int(oet[0]), minute=int(oet[1]), second=int(oet[2]))

    rst = return_start_time.split(":")
    rst = time(hour=int(rst[0]), minute=int(rst[1]), second=int(rst[2]))
    ret = return_end_time.split(":")
    ret = time(hour=int(ret[0]), minute=int(ret[1]), second=int(ret[2]))

    sd = start_date.split("-")
    sd = datetime(year=int(sd[2]), month=int(sd[1]), day=int(sd[0]))
    ed = end_date.split("-")
    ed = datetime(year=int(ed[2]), month=int(ed[1]), day=int(ed[0]))

    c1 = validate_date(start_date)
    c2 = validate_date(end_date)
    c3 = validate_time(onward_start_time)
    c4 = validate_time(onward_end_time)
    c5 = validate_time(return_start_time)
    c6 = validate_time(return_end_time)
    c7 = ost < oet
    c8 = rst < ret
    c9 = sd < ed
    c10 = validate_df(addresses)


    conditions = [c1, c2, c3, c4, c5, c6, c7, c8, c9, c10]

    return ~all(conditions)
        

def convert_to_date(input_date: str):
    input_date = input_date.split("-")
    return datetime(int(input_date[2]), int(input_date[1]), int(input_date[0]))


def convert_to_time(input_time: str):
    input_time = input_time.split(":")
    return time(int(input_time[0]), int(input_time[1]), int(input_time[2]))


def get_holidays_list(start: datetime, stop: datetime):
    num_years = stop.year - start.year

    if num_years != 0:
        years = [start.year + i for i in range(num_years + 1)]
    else:
        years = start.year
    
    holidays_list = holidays.CA(years=years, observed=False)
    holidays_list = [item[0] for item in holidays_list.items()]
    return holidays_list


def generate_dates(start: datetime, stop: datetime):
    canada_holidays_dates = get_holidays_list(start, stop)

    dates = []
    date = start
    num_days = stop.date() - start.date()
    for i in range(num_days.days + 1):
        date = start + timedelta(days=i)

        c1 = date.date() in canada_holidays_dates
        c2 = calendar.day_name[date.weekday()] in ["Saturday", "Sunday"]

        if c1 == False and c2 == False:
            dates.append(date)

    if start in canada_holidays_dates:
        dates.remove(start)
    dates = [datetime.strftime(date, "%d-%m-%Y") for date in dates]

    return dates


def generate_times(start: time, stop: time, num: int):
    times = [
        random_time(
            start=time(hour=start.hour, minute=start.minute, second=start.second), 
            end=time(hour=stop.hour, minute=stop.minute, second=stop.second)) 
        for _ in range(num)
        ]

    return times


def generate_stops(stops_list: list, num: int):
    stops = []
    for i in range(num):
        stops_to_remove = []
        if i % 7 == 0:
            stops_to_remove = random.sample(stops_list, k=1)
            if i % 3 == 0:
                stops_to_remove = random.sample(stops_list, k=2)
        
        day_stops = stops_list
        if len(stops_to_remove) != 0:
            day_stops = list(set(day_stops).difference(set(stops_to_remove)))

        day_stops = ",".join(sorted(day_stops))

        stops.append(day_stops)
    
    return stops


def main():
    start_date = input("Enter the start date in the format 'dd-mm-yyyy': ")
    end_date = input("Enter the end date in the format 'dd-mm-yyyy': ")
    onward_start_time = input("Enter the onward journey start time in the format 'HH:MM:SS' (24 hr): ")
    onward_end_time = input("Enter the onward journey end time in the format 'HH:MM:SS' (24 hr): ")
    return_start_time = input("Enter the return journey start time in the format 'HH:MM:SS' (24 hr): ")
    return_end_time = input("Enter the return journey end time in the format 'HH:MM:SS' (24 hr): ")
    addresses = input("Enter the path to 'addresses.csv': ")

    valid = validate_inputs(start_date, end_date, onward_start_time, onward_end_time, return_start_time, return_end_time, addresses)
    if not valid:
        print("The inputs were not in the correct format, please try again.")
        sys.exit()

    start_date = convert_to_date(start_date)
    end_date = convert_to_date(end_date)

    dates = generate_dates(start_date, end_date)

    onward_start_time = convert_to_time(onward_start_time)
    onward_end_time = convert_to_time(onward_end_time)

    onward_times = generate_times(onward_start_time, onward_end_time, len(dates))

    return_start_time = convert_to_time(return_start_time)
    return_end_time = convert_to_time(return_end_time)
    
    return_times = generate_times(return_start_time, return_end_time, len(dates))

    addresses = pd.read_csv(addresses)

    stops_list = list(addresses["Stop"])
    stops_list.remove("A")
    stops_list.pop()

    stops = generate_stops(stops_list, len(dates))

    timings_df = pd.DataFrame(columns=["Date", "Onward Start Time", "Stops", "Return Start Time"])

    timings_df["Date"] = dates
    timings_df["Onward Start Time"] = onward_times
    timings_df["Stops"] = stops
    timings_df["Return Start Time"] = return_times

    timings_df.to_csv("timings.csv", index=False)
    return 


if __name__ == "__main__":
    main()