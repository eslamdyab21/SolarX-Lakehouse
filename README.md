# SolarX-Lakehouse

# Some background on the data

We start by splitting the `EGY_QH_Helwan` data collected back in 2013, the data is sampled each one hour and is nicely formatted in a csv file after running the `split_weather_data.py` script which makes a csv file for each day in the `weather_history_splitted` directory

```bash
python3 split_weather_data.py
```

![](images/splited_weather_data.png)

<br/>

A sample of the `2013-01-01.csv` data
![](images/sample1.png)

The average size of this data is 23 KB with about 1400 rows, but to truly leverage spark capabilities I resampled this data down to go from frequency by hour to 5 ms, which increased the same day csv file size to around 730 MB with around 16,536001 rows.

The resampling happens with the `resample_weather_data.py` script which takes the above csv file an argument, resample it and write it down to `weather_history_splitted_resampled` folder.

```bash
python3 resample_weather_data.py weather_history_splitted/2013-01-01.csv
```

![](images/resampled_splited_weather_data.png)

<br/>

A sample of the resampled `2013-01-01.csv` data
![](images/sample2.png)

