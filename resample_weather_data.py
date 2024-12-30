import pandas as pd
import sys
import os


def save_file(df, date):
	base_dir = 'lakehouse/spark/weather_history_splitted_resampled/'
	
	if not os.path.exists(base_dir):
		os.makedirs(base_dir)

	df.to_csv(base_dir + date + '.csv', index = False)


def main(weather_data_day_file):
    df = pd.read_csv(weather_data_day_file)
    
    # extract date from argv
    date = weather_data_day_file.split('/')[-1].split('.')[0]
    
    df["timestamp"] = pd.to_datetime(date + " " + df["hour"].astype(str) + ":" + df["minute"].astype(str), format="%Y-%m-%d %H:%M")
    df = df[['timestamp', 'solar_intensity', 'temp']]
    
    # resample to increase frequency from minutes to 5ms
    df.set_index('timestamp', inplace=True)
    df = df.resample('5ms').mean().interpolate()
    df.reset_index(inplace=True)


    # save df into a csv file
    save_file(df, date)
    




if __name__ == "__main__":
    weather_data_day_file = sys.argv[1]
    main(weather_data_day_file)