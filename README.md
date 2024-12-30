# SolarX-Lakehouse
Designing a Lakehouse for extensive data frequency of SoalrX with Spark and Iceberg as big data tools.

**In progress**

<br/>

# Some Background on the Data Sources
## Weather Data

We start by splitting the `EGY_QH_Helwan` data collected back in 2013, the data is sampled each one hour and is nicely formatted in a csv file after running the `split_weather_data.py` script which makes a csv file for each day in the `weather_history_splitted` directory

```bash
python3 split_weather_data.py
```

![](images/splited_weather_data.png)

<br/>

A sample of the `2013-01-01.csv` data
![](images/sample1.png)

The average size of this data is 23 KB with about 1400 rows, but to truly leverage spark capabilities I resampled this data down to go from frequency by hour to 5 ms, which increased the same day csv file size to around `730 MB` with around `16,536001 rows`.

The resampling happens with the `resample_weather_data.py` script which takes the above csv file an argument, resample it and write it down to `lakehouse/spark/weather_history_splitted_resampled` directory.

```bash
python3 resample_weather_data.py weather_history_splitted/2013-01-01.csv
```

![](images/resampled_splited_weather_data.png)

<br/>

A sample of the resampled `2013-01-01.csv` data
![](images/sample2.png)

<br/>
<br/>

## Home Data

We could've grabed some home power usage data from the internet, but to make it more customized and variable, the home power usage is calculated based on the data provided in a `json` file `home_appliances_consumption.json`, in it we add the different devices we have in our home and their corresponding hourly power rating and the time of use.

```json
{
	"Refrigerator":	
		{
		"consumption": [300,1500],
		"time": "00:00-24:00"
		},
	"Electric Oven":
		{
		"consumption": [2000,5000],
		"time": "16:00-16:30,21:00-21:30"
		},
	"Electric Kettle":
		{
		"consumption": [1500,1500],
		"time": "07:00-07:15,12:00-12:15,16:30-17:00,21:30-22:00"
		},
	"Air Conditioner":
		{
		"consumption": [500,3000],
		"time": "00:00-24:00"
		},
}
```

Above is a sample of to get an idea, `consumption` have a minimum and maximum value a device can draw, and in `time` we specify ranges each separated with a `,`.

<br/>

The data is also resampled to `5ms` like the weather data for the same reason and it's size this time is around `1.4GB`

The process happens using the `home_power_usage.py` script which takes the same above weather csv file an argument and write the home usage down to `lakehouse/spark/home_power_usage_history` directory, the weather csv file purpose is just to extract the corresponding date.

```bash
python3 home_power_usage.py weather_history_splitted/2013-01-03.csv
```

A sample of the resampled `2013-01-01.csv` data and files sizes

![](images/home_data.png)

<br/>
<br/>
<br/>

# Cluster Configuration Setup
In the docker compose file, there are 4 workers and one master, `spark-worker-1` to `spark-worker-4`. We can specify the default memory and cores in the environment variables below.

<br/>

The setup is 4 workers each with 1G of memory and 2 cores, and example of the worker service is below.
```docker
spark-worker-1:
	image: tabulario/spark-iceberg
	container_name: spark-worker-1
	build: spark/

	volumes:
		- ./spark_workers.sh:/opt/spark/spark_workers.sh
	
	networks:
		- iceberg_net
	
	environment:
		- AWS_ACCESS_KEY_ID=admin		
		- AWS_SECRET_ACCESS_KEY=password
		- AWS_REGION=us-east-1
		- SPARK_MODE=worker
		- SPARK_MASTER_URL=spark://spark-master:7077
		- SPARK_WORKER_CORES=2
		- SPARK_WORKER_MEMORY=1G
	
	depends_on:
		- spark-master
```

<br/>

### To start the cluster:
**An important note**: **make sure your machine have these extra resources for the workers, if not then remove some of the workers or allocate fewer cpu and memory for them**

First we run the docker compose file which will start the spark master, workers and iceberg 
```bash
docker compose up
```

```bash
docker ps

spark-worker-4
	Container ID: 1e04615dcfca
	Command: "./entrypoint.sh not…"
	Image: tabulario/spark-iceberg
	CreatedAt: 2024-12-26 18:34:31 +0200 EET
	Status: Up 12 minutes
	Ports: 
	
spark-worker-1
	Container ID: 89ffe36c4c4d
	Command: "./entrypoint.sh not…"
	Image: tabulario/spark-iceberg
	CreatedAt: 2024-12-26 18:34:31 +0200 EET
	Status: Up 12 minutes
	Ports: 
	
spark-worker-2
	Container ID: 0c7fdc403ff0
	Command: "./entrypoint.sh not…"
	Image: tabulario/spark-iceberg
	CreatedAt: 2024-12-26 18:34:31 +0200 EET
	Status: Up 12 minutes
	Ports: 
	
spark-worker-3
	Container ID: 70fedbe3d4f7
	Command: "./entrypoint.sh not…"
	Image: tabulario/spark-iceberg
	CreatedAt: 2024-12-26 18:34:31 +0200 EET
	Status: Up 12 minutes
	Ports: 
	
mc
	Container ID: 6dcc184f047c
	Command: "/bin/sh -c ' until …"
	Image: minio/mc
	CreatedAt: 2024-12-26 18:34:31 +0200 EET
	Status: Up 12 minutes
	Ports: 
	
spark-master
	Container ID: 9252dc083912
	Command: "./entrypoint.sh not…"
	Image: tabulario/spark-iceberg
	CreatedAt: 2024-12-26 18:34:31 +0200 EET
	Status: Up 12 minutes
	Ports: 0.0.0.0:8080->8080/tcp, :::8080->8080/tcp, 0.0.0.0:8888->8888/tcp, :::8888->8888/tcp, 0.0.0.0:10000-10001->10000-10001/tcp, :::10000-10001->10000-10001/tcp
	
minio
	Container ID: 9c066befff9e
	Command: "/usr/bin/docker-ent…"
	Image: minio/minio
	CreatedAt: 2024-12-26 18:34:31 +0200 EET
	Status: Up 12 minutes
	Ports: 0.0.0.0:9000-9001->9000-9001/tcp, :::9000-9001->9000-9001/tcp
	
iceberg-rest
	Container ID: 143c5c0a1703
	Command: "java -jar iceberg-r…"
	Image: apache/iceberg-rest-fixture
	CreatedAt: 2024-12-26 18:34:31 +0200 EET
	Status: Up 12 minutes (healthy)
	Ports: 0.0.0.0:8181->8181/tcp, :::8181->8181/tcp
```

<br/>

There is one other step we need to do, connect the workers with the master, we do that using the `spark_workers.sh` bash script.
```bash
docker exec -it spark-worker-1 /bin/bash -c "chmod +x /opt/spark/spark_workers.sh && /opt/spark/spark_workers.sh"
```

```bash
docker exec -it spark-worker-2 /bin/bash -c "chmod +x /opt/spark/spark_workers.sh && /opt/spark/spark_workers.sh"
```

```bash
docker exec -it spark-worker-3 /bin/bash -c "chmod +x /opt/spark/spark_workers.sh && /opt/spark/spark_workers.sh"
```

```bash
docker exec -it spark-worker-4 /bin/bash -c "chmod +x /opt/spark/spark_workers.sh && /opt/spark/spark_workers.sh"
```

Navigate to the url `127.0.0.1:8080` in which the spark master is running, make a note of the spark master internal url `spark://9611ff031a11:7077`, we will need it in the session creation step below.
![](images/workers.png)
We can see that all worker is now recognized by the master.

<br/>

Note that we could've just run the script as a command in docker compose file, but some reason it doesn't work.

<br/>
<br/>

# Derive Solar Panel Readings Fata with Spark
Here we start working on the 730 MB and 16,536001 records of data of first day, we start by setting up the cluster, the choice was 3 workers with 6 executors each with 1 core and 512M for memory.

```python
spark = (
    SparkSession
    .builder
    .appName("Solar Power")
    .master("spark://9611ff031a11:7077")
    .config("spark.executor.cores", 1)
    .config("spark.cores.max", 6)
    .config("spark.executor.memory", "512M")
    .getOrCreate()
)
```

<br/>

After creating the spark session, here with app name `Solar Power`, you can navigate to this session related app jobs and stages details on `127.0.0.1:4041` to better understand how the job is working and optimize it.

Then we load the csv file and partition with a derived column `hour`, this partitioning will ensure data spreading without skewing and will help down the line in processing, also it will prevent memory spill.

```python
_schema = "timestamp timestamp, solar_intensity float, temp float"

weather_df = spark.read.format("csv").schema(_schema).option("header", True)\
                   .load("/home/iceberg/warehouse/weather_history_splitted_resampled/2013-01-01.csv")\
                   .withColumn("hour", F.hour(F.col("timestamp")))
```

Then we do the calculations to drive the solar panel power for each reading, we assume an approximate linear model in the calculations, a good enough approximation since it's not really the point here, then we save the data as csv, we will use parquet format down the line.

<br/>

Here is the explain plan, only one necessary exchange (shuffling) at the begging. 
![](images/plan.png)

And here are the files saved partitioned by hour of the day with some sizes of them, it's on the order of `20M` to `40M` each.
![](images/solar_readings_files.png)

<br/>

## Optimization in this step
![](images/spill_disk.png)
As you can see there is a spill both in memory and disk, which is very expensive, the job took about `25` seconds, to solve this problem we have a number of options:

- Increase the number of executors and their memory (we can't here I've limited resources in my laptop)
- increase the number of partitions

I went with option 2, I increased the number of partitions from 23 to 92, 92 being the number of every 15 minutes time interval of the day.
```python
_schema = "timestamp timestamp, solar_intensity float, temp float"

weather_df = spark.read.format("csv").schema(_schema).option("header", True)\
                   .load("/home/iceberg/warehouse/weather_history_splitted_resampled/2013-01-01.csv")\
                   .withColumn("15_min_interval", F.floor((F.hour(F.col("timestamp"))*60 + F.minute(F.col("timestamp")) - 60) / 15))
```

```python
spark.conf.set("spark.sql.shuffle.partitions", 92)
weather_partitioned_df = weather_df.repartition(92, F.col('15_min_interval'))
```

```python
solar_panel_readings_df.write.format("csv").option("header", True).mode("overwrite").partitionBy("15_min_interval") \
                       .save("/home/iceberg/warehouse/weather_history_splitted_resampled/solar_panel_readings/2013-01-01.csv")
```

![](images/no_spill.png)
And now there is no spill in memory and disk, as a result the job went from taking `25` seconds to just `14` seconds, and the size per partition is also decreased down to the range from `5MB` to `10MB`.
![](images/14sec.png)


Now to have a structured lakehouse with tables format and to have some management and governance on the data, we will need a tool to help orchestrate and facilitate that, if continue saving the data like we did above it will quickly become a mess without versioning and also it will be a hustle to update data (overwriting). 

So going forward will use `iceberg` for that, iceberg also have a nice api that we can use to query the data saved on disk in csv or parquet format as if it was a table with normal sql, so it will feel homey for the analytical team.


<br/>
<br/>

# Lakehouse Raw Records 
##  Solar Tables

You can find the code in the `notebooks/solar_panel_iceberg_tables.ipynb` which will be a `.py` file later with the others to run the pipeline with `Airflow`.

We start with those two tables solar_panel and solar_panel_readings
```sql 
%%sql

CREATE TABLE SolarX_Raw_Transactions.solar_panel(
    id INT,
    name VARCHAR(25) NOT NULL,
    capacity_kwh FLOAT NOT NULL,
    intensity_power_rating FLOAT NOT NULL,
    temperature_power_rating FLOAT NOT NULL
)
USING iceberg
```

```sql
%%sql

CREATE TABLE SolarX_Raw_Transactions.solar_panel_readings(
    timestamp TIMESTAMP NOT NULL,
    15_minutes_interval INT NOT NULL,
    panel_id INT NOT NULL,
    generated_power_amount FLOAT NOT NULL
)
USING iceberg
PARTITIONED BY (DAY(timestamp), 15_minutes_interval);
```

We are partitioning the raw readings on both the `day` and the `15_minutes_interval`, and the power is calculated same as before, the only difference is that now we have 3 solar panels and instead of saving the data into `csvs`, we are saving them with `iceberg`, and iceberg under the hod saves them in `parquet` format. 

<br/>

We are partitioning the raw readings on both the `day` and the `15_minutes_interval`, that would be a lot of partitions you might say in the long run, we would only keep the last 7 days of raw data in our lakehouse, because after this one week period we generally won't be interested in the high frequency data and also to minimize space cost.

Instead we will save the past data in the `15_minutes_interval` frequency in another table in the warehouse, and this table is what will be used in the analytics.

<br/>

We can see here on `127.0.0.1:9000` using the `minio` service in the docker compose, log in with username and password specified in the docker compose file, after creating the two tables in the jupyter notebook, All raw data will live inside `Raw Transactions` and later we will make a new dir/table for the warehouse low frequency aggregated data for analytics.

![](images/raw_trx.png)
Inside each directory exists two dirs, `data` and `metadata`, the data files will be organized using the partitioning we provided, fore example here after inserting the first solar panel data
```python
panel_id = 1
solar_panel_readings_df1 = calc_solar_readings(panel_id, weather_partitioned_df)
solar_panel_readings_df1.createOrReplaceTempView("temp_view_1")
```

```sql
%%sql
    
INSERT INTO SolarX_Raw_Transactions.solar_panel_readings (timestamp, 15_minutes_interval, panel_id, generated_power_amount)
SELECT timestamp                  as timestamp,
       15_min_interval            as 15_minutes_interval,
       1                          as panel_id,
       current_generation_watt    as generated_power_amount
       
FROM temp_view_1
```

![](images/panel1.png)

And after inserting the other two solar panels data, another two files get created.
![](images/panel23.png)

Same structure will apply if we were using a cloud storage based service like amazon S3 for example, also these data can also be inspected form the container `minio`
![](images/minio.png)

<br/>

Also and interesting observation, the csv files of one solar panel data of one day was around `600MB` in size, now the 3 solar panels data combined is only around `171.6MB` in size, this nice reduction in size comes from the fact that iceberg saves the data in `parquet` format and this format uses `run length encoding` which can reduce the size of the data if the low cardinality data are grouped together, and that is the case in our data, the average sun hours is something like `11` hours a day, and the rest is just `zero`, so the solar power generated is zero in the rest, and we're partitioning in way that also groups those zeros together to achieve this reduction in size.


<br/>
<br/>

##  Home Power Usage Tables
You can find the code in the `notebooks/home_power_load_iceberg_tables.ipynb` which will be a `.py` file later with the others to run the pipeline with `Airflow`.

Only one table is used here which has the power usage related data
```sql
%%sql

CREATE TABLE SolarX_Raw_Transactions.home_power_readings(
    timestamp               TIMESTAMP NOT NULL,
    15_minutes_interval     SMALLINT  NOT NULL,
    min_consumption_wh      FLOAT     NOT NULL,
    max_consumption_wh      FLOAT     NOT NULL
)
USING iceberg
PARTITIONED BY (DAY(timestamp), 15_minutes_interval);
```

We are partitioning the raw readings on both the `day` and the `15_minutes_interval`, that would be a lot of partitions you might say in the long run, we would only keep the last 7 days of raw data in our lakehouse, because after this one week period we generally won't be interested in the high frequency data and also to minimize space cost.

Instead we will save the past data in the 15_minutes_interval frequency in another table in the warehouse, and this table is what will be used in the analytics.

![](images/minio_home.png)

Also the 3 days worth of data size combined is only `350MB` compared to one day of data in the source making `1.4GB`.

<br/>

Some quick analysis
```sql
%%sql

SELECT 
    DAY(timestamp) as day, 
    SUM(min_consumption_wh)/1000 as min_consumption_kwh, 
    SUM(max_consumption_wh)/1000 as max_consumption_kwh
FROM SolarX_Raw_Transactions.home_power_readings
GROUP BY day

|day|min_consumption_kwh|max_consumption_kwh|
|-|------------------|------------------|
|1|31.030535656945837|126.82835777154983|
|2|33.01199622871832 |121.20579759246623|
|3|31.291361060320924|121.99561212848174|
```

