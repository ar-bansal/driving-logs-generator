from DrivingLogGenerator.functions import *
import asyncio


async def main():
    print("App started.                     ")
    print()

    addresses, timings, api_key = take_path_inputs()

    print("Validating the input files...    ", end="\r")

    if validate_inputs(addresses, timings, api_key):
        print("Inputs validated.                ")
        print("Generating driving data...       ", end="\r")

        with open(api_key, "r") as file:
            api_key = file.read()

        gmaps = googlemaps.Client(key=api_key)
        addresses = pd.read_csv(addresses) 
        timings = pd.read_csv(timings)

        timings_1 = format_timestamps(timings)
        timings_2 = format_stops(timings_1, addresses)

        loop = asyncio.get_event_loop()
        with ProcessPoolExecutor() as executor:
            futures = [
                loop.run_in_executor(
                    executor, 
                    get_day_data, 
                    *(day["Onward"], day["stops"].split(","), day["Return"], addresses, gmaps)
                ) 
                for index, day in timings_2.iterrows()]
        day_wise_results = await asyncio.gather(*futures)

        data = pd.concat(day_wise_results, axis=0)

        save_output(data)

        print("Driving data generated.          ")
        print("\nThe output data can be viewed at:")
        print(f"'{os.getcwd()}/{OUTPUT_PATH}'")

        logging.info("Data generation successful.")
        logging.info("The output file can be found at:")
        logging.info(f"{os.getcwd()}/{OUTPUT_PATH}")
    else:
        print("App exited due to error. Please check the logs for more information:")
        print(f"'{os.getcwd()}/{LOG_PATH}'")
    
    print()
    input("Press 'Enter' to exit.")
    logging.info("===================================================== App Exited Successfully =====================================================")


if __name__ == "__main__":
    asyncio.run(main())