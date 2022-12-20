import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import googlemaps
import asyncio
from concurrent.futures import ProcessPoolExecutor


# gmaps = googlemaps.Client(key="AIzaSyBFFENf2CZBEYbuvJPlQdcVnFVm2XTWPxs")
def padding(string: str) -> str:
    """
    Pad strings with a zero, for the correct filename formatting. 
    """
    if len(string) == 1:
        string = f"0{string}"
    return string


RUN_DATE = datetime.now()

output_name = f"driving_data_{padding(str(RUN_DATE.year))}{padding(str(RUN_DATE.month))}{padding(str(RUN_DATE.day))}_{padding(str(RUN_DATE.hour))}{padding(str(RUN_DATE.minute))}{padding(str(RUN_DATE.second))}"
OUTPUT_PATH = f"output/{output_name}.csv"
LOG_PATH = f"driving_app_logs/app_logs_{padding(str(RUN_DATE.year))}{padding(str(RUN_DATE.month))}{padding(str(RUN_DATE.day))}.log"

if not "driving_app_logs" in os.listdir(os.getcwd()):
    os.mkdir(f"{os.getcwd()}/driving_app_logs/")

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s", 
    level=logging.INFO, 
    datefmt="%d-%b-%y %H:%M:%S", 
    filename=LOG_PATH, 
    filemode="a")

logging.info("===================================================== App Started Successfully =====================================================")


def validate_inputs(addresses: pd.DataFrame, timings: pd.DataFrame, api_key: str) -> bool:
    """
    Validate if the input files have the required format. 

    Parameters

    timings: pd.DataFrame
        Dataframe containing information about trip timings.
    addresses: pd.DataFrame
        Dataframe containing information about the origin, destination and waypoint addresses.
    api_key: str
        Google Maps API key.
    """

    try:
        addresses = pd.read_csv(addresses)
    except Exception:
        logging.exception("Could not read 'addresses.csv'. Please check the file name and location.")
    
    try:
        timings = pd.read_csv(timings)
    except Exception:
        logging.exception("Could not read 'addresses.csv'. Please check the file name and location.")

    try:
        with open(api_key, "r") as file:
            api_key = file.read()
        client = googlemaps.Client(key=api_key)
    except Exception:
        logging.exception("Error encountered while trying to connect to Google Maps.")
        logging.info("===================================================== App Exited Due To Error =====================================================")

    try:
        addresses_valid_cols = ["Stop", "Address"]
        timings_valid_cols = ["Date", "Onward Start Time", "Stops", "Return Start Time"]

        c1 = (addresses.columns == addresses_valid_cols).all()
        c2 = (timings.columns == timings_valid_cols).all()
        c3 = ~addresses.isnull().any().any()
        c4 = ~timings.isnull().any().any()

        if ~c1:
            logging.error("Column names are incorrect in 'addresses.csv'. Ensure there are no extra spaces after the column name in the sheet.")

        if ~c2:
            logging.error("Column names are incorrect in 'timings.csv'. Ensure there are no extra spaces after the column name in the sheet.")

        if ~c3:
            logging.error("'addresses.csv' has incomplete data. Please ensure all required data is present.")

        if ~c4: 
            logging.error("'timings.csv' has incomplete data. Please ensure all required data is present.")

        conditions = [c1, c2, c3, c4]
        
        if all(conditions):
            valid = True
            logging.info("Input data is in the correct format.")
        else:
            valid = False
            logging.exception("Input data does not follow the correct format, or the data is incomplete.")
            logging.info("")
    except Exception as e:
        valid = False
        logging.exception("Error encountered while trying to validate input data:")
        
    return valid


def secs_to_ts(secs: int) -> tuple:
    """
    Convert seconds (int) to HH:MM:SS format timestamp.

    Parameters

    secs: int
        Seconds as an integer.
    """
    hours = secs//3600 
    minutes = int((secs / 3600 - hours) * 60)
    seconds = secs % 60 % 60

    return (hours, minutes, seconds)


def format_timestamps(timings: pd.DataFrame) -> pd.DataFrame: 
    """
    Join date and start times and convert to timestamp.

    Parameters

    timings: pd.DataFrame
        Dataframe containing information about trip timings.
    """

    formatted = pd.DataFrame()

    formatted["Onward"] = timings.apply(lambda x: x["Date"] + " " + x["Onward Start Time"], axis=1)
    formatted["Onward"] = formatted["Onward"].apply(lambda x: datetime.strptime(x, "%d-%m-%Y %H:%M:%S"))

    formatted["Return"] = timings.apply(lambda x: x["Date"] + " " + x["Return Start Time"], axis=1)
    formatted["Return"] = formatted["Return"].apply(lambda x: datetime.strptime(x, "%d-%m-%Y %H:%M:%S"))
    
    formatted["Stops"] = timings["Stops"]

    return formatted


def format_stops(timings: pd.DataFrame, addresses: pd.DataFrame) -> pd.DataFrame:
    """
    Add origin and destination to stops in the timings DataFrame from the addresses DataFrame. 

    Parameters

    direction: str
        Valid values are "Onward" and "Return".
    timings: pd.DataFrame
        Dataframe containing information about trip timings.
    addresses: pd.DataFrame
        Dataframe containing information about the origin, destination and waypoint addresses.
    """

    formatted = timings.copy(deep=True)
    final_destination = addresses.loc[len(addresses) - 1, "Stop"]
    formatted["stops"] = timings["Stops"].apply(lambda x: f"A,{x},{final_destination}")
        
    return formatted


def get_segment_data(departure_time: datetime, start: str, stop: str, addresses: pd.DataFrame, client: googlemaps.Client) -> dict:
    """
    Get cost of driving from start location to stop location when starting at departure_time, from Google Maps API.

    Parameters

    departure_time: datetime.datetime
        Time of departure. If less than datetime.datetime.now(), the time of departures's year will be changed to the next year. 
    start: str
        Origin of the trip.
    stop: str
        Destination of the trip.
    addresses: pd.DataFrame
        Dataframe containing information about the origin, destination and waypoint addresses.
    """
    # Get timestamp for departure
    actual_departure_time = departure_time
    now = datetime.now()
    if departure_time < now:
        departure_time = datetime(
            now.year + 1, 
            departure_time.month, 
            departure_time.day, 
            departure_time.hour, 
            departure_time.minute, 
            departure_time.second)

    # Map locations using addresses
    start_add = addresses.loc[addresses["Stop"]==start, "Address"].values[0]
    stop_add = addresses.loc[addresses["Stop"]==stop, "Address"].values[0]

    # Call Google Maps API
    result = client.directions(
            start_add, 
            stop_add, 
            mode="driving", 
            departure_time=departure_time, 
            traffic_model="best_guess")

    # Get cost from response
    driving_time = result[0]["legs"][0]["duration"]["value"]
    driving_distance = result[0]["legs"][0]["distance"]["value"]

    driving_time = secs_to_ts(driving_time)
    arrival_time = actual_departure_time + timedelta(hours=driving_time[0], minutes=driving_time[1], seconds=driving_time[2])

    output = {
        "Start Date": actual_departure_time,
        "End Date": arrival_time,
        "Start Location": start_add, 
        "Stop Location": stop_add, 
        "Distance": driving_distance/1000
        }

    return output


def get_direction_data(departure_time: datetime, stops_list: list, addresses: pd.DataFrame, client: googlemaps.Client) -> list:
    """
    Get cost of driving from origin to destination via waypoints, starting the journey at departure_time. 
    A waiting time of upto 2 minutes is randomly generated at the waypoints.

    Parameters

    departure_time: datetime.datetime
        Time of departure. If less than datetime.datetime.now(), the time of departures's year will be changed to the next year. 
    stops_list: list
        List of stops. The first stop is the origin, and the last stop is the destination. All intermediate stops are waypoints.
    addresses: pd.DataFrame
        Dataframe containing information about the origin, destination and waypoint addresses.
    """
    # Get waiting time at a stop
    waiting_time = np.random.randint(5, 121)
    waiting_time = secs_to_ts(waiting_time)

    next_departure_time = departure_time
    outputs = []

    # Iterated over stops_list to get segment data
    for i in range(len(stops_list) - 1):
        start = stops_list[i]
        stop = stops_list[i + 1]

        segment_data = get_segment_data(
                departure_time=next_departure_time,
                start=start, 
                stop=stop,
                addresses=addresses, 
                client=client
                )

        outputs.append(segment_data)

        # Set next departure time
        next_departure_time = segment_data["End Date"] + timedelta(hours=waiting_time[0], minutes=waiting_time[1], seconds=waiting_time[2])

    return outputs


def get_day_data(onward_departure_time: datetime, stops_list: list, return_departure_time: datetime, addresses: pd.DataFrame, client: googlemaps.Client) -> pd.DataFrame:
    """
    Get cost of making a return trip from origin-destination-origin via waypoints. 

    Parameters

    onward_departure_time: datetime
        Time of departure from origin.
    stops_list: list
        List of stops. The first stop is the origin, and the last stop is the destination. All intermediate stops are waypoints.
    return_departure_time: datetime
        Time of departure from destination.
    addresses: pd.DataFrame
        Dataframe containing information about the origin, destination and waypoint addresses.
    """
    # Get onward journey data
    onward_list = get_direction_data(onward_departure_time, stops_list, addresses, client)
    onward_df = pd.DataFrame.from_dict(onward_list)

    # Get return journey data
    return_list = get_direction_data(return_departure_time, list(reversed(stops_list)), addresses, client)
    return_df = pd.DataFrame.from_dict(return_list)

    # Create a single dataframe for the day's data
    day_trips = pd.concat([onward_df, return_df], axis=0)

    return day_trips


async def get_data(timings: pd.DataFrame, addresses: pd.DataFrame, client: googlemaps.Client) -> pd.DataFrame:
    """
    Find cost of driving from origin to destination and back to origin via the waypoints, for all available dates.

    Parameters

    timings: pd.DataFrame
        Dataframe containing information about the date, onward start time, stops and return start time.
    addresses: pd.DataFrame
        Dataframe containing information about the origin, destination and waypoint addresses.
    """
    loop = asyncio.get_event_loop()
    with ProcessPoolExecutor() as executor:
        futures = [
            loop.run_in_executor(
                executor, 
                get_day_data, 
                *(day["Onward"], day["stops"].split(","), day["Return"], addresses, client)
            ) 
            for index, day in timings.iterrows()]
        day_wise_results = await asyncio.gather(*futures)

        data = pd.concat(day_wise_results, axis=0)

        return data


def take_path_inputs():
    addresses = input("Enter the path to 'addresses.csv': ")
    timings = input("Enter the path to 'timings.csv': ")
    api_key = input("Enter the path to 'api_key.txt': ")
    print()

    return addresses, timings, api_key


def save_output(dataframe: pd.DataFrame):
    if not "output" in os.listdir(os.getcwd()):
        os.mkdir(f"{os.getcwd()}/output/")
    dataframe.to_csv(OUTPUT_PATH, index=False)
    return



# async def main():
#     print("App started.                     ")
#     print()

#     addresses, timings, api_key = take_path_inputs()

#     print("Validating the input files...    ", end="\r")

#     if validate_inputs(addresses, timings, api_key):
#         print("Inputs validated.                ")
#         print("Generating driving data...       ", end="\r")

#         with open(api_key, "r") as file:
#             api_key = file.read()

#         gmaps = googlemaps.Client(key=api_key)
#         addresses = pd.read_csv(addresses) 
#         timings = pd.read_csv(timings)

#         output = asyncio.run(get_data(timings, addresses, gmaps))

#         save_output(output)

#         print("Driving data generated.          ")
#         print("\nThe output data can be viewed at:")
#         print(f"'{os.getcwd()}/{OUTPUT_PATH}'")

#         logging.info("Data generation successful.")
#         logging.info("The output file can be found at:")
#         logging.info(f"{os.getcwd()}/{OUTPUT_PATH}")
#     else:
#         print("App exited due to error. Please check the logs for more information:")
#         print(f"'{os.getcwd()}/{LOG_PATH}'")
    
#     print()
#     input("Press 'Enter' to exit.")
#     logging.info("===================================================== App Exited Successfully =====================================================")


# if __name__ == "__main__":
#     timings_df = pd.read_csv(r"C:\Users\aryan\Downloads\Personal Projects\Freelancing\Social More Marketing Agency\dev\v3\input\timings.csv")
#     addresses_df = pd.read_csv(r"C:\Users\aryan\Downloads\Personal Projects\Freelancing\Social More Marketing Agency\dev\v3\input\addresses.csv")

#     timings_df1 = format_timestamps(timings_df)
#     timings_df2 = format_stops("Onward", timings_df1, addresses_df)

#     dep_time = timings_df2.loc[0, "Onward"]
#     ret_time = timings_df2.loc[0, "Return"]
#     start = timings_df2.loc[0, "stops"].split(",")[0]
#     stop = timings_df2.loc[0, "stops"].split(",")[1]

#     # addresses_df_indexed = addresses_df.copy(deep=True)

#     # print(func1(dep_time, start, stop, addresses_df, gmaps))
#     print(timings_df2.loc[0, "stops"].split(","))


#     # print(func2(dep_time, timings_df2.loc[0, "stops"].split(","), addresses_df, gmaps))

#     # print(func3(dep_time, timings_df2.loc[0, "stops"].split(","), ret_time, addresses_df, gmaps))

#     gmaps = googlemaps.Client(key="AIzaSyBFFENf2CZBEYbuvJPlQdcVnFVm2XTWPxs")
#     test = asyncio.run(get_data(timings_df2, addresses_df, gmaps))
#     print(test)
#     # temp_f(timings_df2)
#     # print(func5(timings_df2, addresses_df, gmaps))
