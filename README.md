# Driving Logs Generator
This is the code base to generate cost (time and distance) to travel from an origin to a destination, while stopping at waypoints, using Google Maps API.

This project uses Google Maps API to predict the cost of driving from the origin to the destination via waypoints. 

Features:
1. Predict cost by giving input as:
    origin, waypoints and destination addresses -> input/addresses.csv
    date, onward start time, stops and return start time -> input/timings.csv
2. Randomly generate waiting time of upto 2 minutes per stop. 
3. Generate and save prediction in the "output/" directory (Create the directory if not found).
4. Generate and save logs about app runs in the "log/" directory (Create the directory if not found).


Building the distributable file:
1. Navigate to the directory containing the cli.py file.
2. Run the following command: 
    `pyinstaller cli.py -r requirements.txt --hidden-import=googlemaps --onefile --name=DrivingLogGenerator`
3. To generate the distributable file using the .spec file, run the following command:
    `pyinstaller DrivingLogGenerator_linux.spec`
    

Usage:
1. Linux:
    1. Give permissions to run the executable file: chmod a+x DrivingLogGenerator_linux
    2. Run the file using the terminal: ./DrivingLogGenerator
    3. Run the file without the terminal: Run the DrivingLogGenerator file.
