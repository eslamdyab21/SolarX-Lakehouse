import json
import random
import sys
import os
import pandas as pd

def load_home_load():
    with open('home_appliances_consumption.json') as f:
        HOME_USAGE_POWER = json.load(f)

    return HOME_USAGE_POWER


def save_file(df, home_data_day_file):
	base_dir = 'lakehouse/spark/home_power_usage_history/'
	date = home_data_day_file.split('/')[-1].split('.')[0]
     
	if not os.path.exists(base_dir):
		os.makedirs(base_dir)

	df.to_csv(base_dir + date + '.csv', index = False)
     

def resample(df, home_data_day_file):
    # extract date from argv
    date = home_data_day_file.split('/')[-1].split('.')[0]
    df["timestamp"] = pd.to_datetime(date + " " + df["hour"].astype(str), format="%Y-%m-%d %H:%M")
    
    df = df.sort_values(by='timestamp')
    del df['hour']

    df.set_index('timestamp', inplace=True)
    

    df = df.resample('5ms').mean().interpolate()
    df.reset_index(inplace=True)

    return df


def generate_power_usage(df):
    expanded_rows = []
    for _, row in df.iterrows():
        time_ranges = row["usage_time"].split(',')
        for time_range in time_ranges:
            start, end = time_range.split('-')
            start_hour = int(start.split(':')[0])
            end_hour = int(end.split(':')[0]) + (1 if int(end.split(':')[1]) > 0 else 0)  # Include minutes
            
            for hour in range(start_hour, end_hour):
                if hour == 0:
                    continue
                elif hour == 23:
                    minute = '59'
                else:
                    minute = '00'

                min_consumption = row["min_consumption_rating"] * random.uniform(0.5, 1.5)
                max_consumption = row["max_consumption_rating"] * random.uniform(0.5, 1.5)
                avg_consumption = (min_consumption + max_consumption) / 2
                expanded_rows.append({
                    "hour": str(hour) + ':' + minute,
                    "min_consumption_wh": min_consumption,
                    "max_consumption_wh": max_consumption,
                    "avg_consumption_wh": avg_consumption
                })
    
    power_usage_df = pd.DataFrame.from_dict(expanded_rows)

    return power_usage_df.groupby('hour', as_index = False).sum()




if __name__ == "__main__":
    home_data_day_file = sys.argv[1]
    HOME_USAGE_POWER = load_home_load()

    df = pd.DataFrame([
        {
            "name": name,
            "min_consumption_rating": info["consumption"][0],
            "max_consumption_rating": info["consumption"][1],
            "usage_time": info["time"]
        }
        for name, info in HOME_USAGE_POWER.items()
    ])

    power_usage_df = generate_power_usage(df)
    power_usage_resampled_df = resample(power_usage_df, home_data_day_file)
    save_file(power_usage_resampled_df, home_data_day_file)
