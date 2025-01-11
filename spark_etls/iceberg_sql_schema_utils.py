
# -------------- SolarX_Raw_Transactions tables -------------
create_raw_namespace = """
CREATE DATABASE IF NOT EXISTS SolarX_Raw_Transactions;
"""


create_raw_home_power_readings_schema = """
CREATE TABLE SolarX_Raw_Transactions.home_power_readings(
    timestamp               TIMESTAMP NOT NULL,
    15_minutes_interval     SMALLINT  NOT NULL,
    min_consumption_wh      FLOAT     NOT NULL,
    max_consumption_wh      FLOAT     NOT NULL
)
USING iceberg
PARTITIONED BY (DAY(timestamp), 15_minutes_interval);
"""


create_raw_solar_panel_schema = """
CREATE TABLE SolarX_Raw_Transactions.solar_panel(
    id INT,
    name VARCHAR(25) NOT NULL,
    capacity_kwh FLOAT NOT NULL,
    intensity_power_rating FLOAT NOT NULL,
    temperature_power_rating FLOAT NOT NULL
)
USING iceberg
"""


create_raw_solar_panel_power_readings_schema = """
CREATE TABLE SolarX_Raw_Transactions.solar_panel_readings(
    timestamp TIMESTAMP NOT NULL,
    15_minutes_interval INT NOT NULL,
    panel_id INT NOT NULL,
    generation_power_wh FLOAT NOT NULL
)
USING iceberg
PARTITIONED BY (DAY(timestamp), panel_id, 15_minutes_interval);
"""

# -------------- SolarX_WH tables -------------

create_wh_namespace = """
CREATE DATABASE IF NOT EXISTS SolarX_WH;
"""


create_wh_dim_home_schema = """
CREATE TABLE SolarX_WH.dim_home(
    home_key                             SMALLINT    NOT NULL,
    home_id                              SMALLINT    NOT NULL,
    min_consumption_power_wh             FLOAT       NOT NULL,
    max_consumption_power_wh             FLOAT       NOT NULL,

    -- scd type2 for min_consumption_power_wh
    start_date                           TIMESTAMP   NOT NULL,
    end_date                             TIMESTAMP,

    current_flag                         BOOLEAN NOT NULL
)
USING iceberg;
"""


create_wh_dim_home_appliances_schema = """
CREATE TABLE SolarX_WH.dim_home_appliances(
    home_appliance_key                  SMALLINT    NOT NULL,
    home_key                            SMALLINT    NOT NULL, -- REFERENCES dim_home(home_key)
    appliance                           VARCHAR(25) NOT NULL,    
    min_consumption_power_wh            FLOAT       NOT NULL,
    max_consumption_power_wh            FLOAT       NOT NULL,
    usage_time                          VARCHAR(50) NOT NULL
)
USING iceberg;
"""


create_wh_fact_home_power_readings_schema = """
CREATE TABLE SolarX_WH.fact_home_power_readings(
    home_power_reading_key          TIMESTAMP     NOT NULL,
    home_key                        SMALLINT      NOT NULL,   -- REFERENCES dim_home(home_key)
    date_key                        TIMESTAMP     NOT NULL,   -- REFERENCES dim_date(date_key)

    min_consumption_power_wh        FLOAT         NOT NULL,
    max_consumption_power_wh        FLOAT         NOT NULL 
)
USING iceberg
PARTITIONED BY (MONTH(date_key))
"""


create_wh_dim_solar_panel_schema = """
CREATE TABLE SolarX_WH.dim_solar_panel(
    solar_panel_key                             INT         NOT NULL,
    solar_panel_id                              SMALLINT    NOT NULL,
    name                                        VARCHAR(20) NOT NULL,    
    capacity_kwh                                FLOAT       NOT NULL,
    intensity_power_rating_wh                   FLOAT       NOT NULL,
    temperature_power_rating_c                  FLOAT       NOT NULL,

    -- scd type2
    start_date                                  TIMESTAMP   NOT NULL,
    end_date                                    TIMESTAMP,

    current_flag                                BOOLEAN
)
USING iceberg;
"""


create_wh_fact_solar_panel_power_readings_schema = """
CREATE TABLE SolarX_WH.fact_solar_panel_power_readings(
    solar_panel_key                 SMALLINT      NOT NULL,   -- REFERENCES dim_solar_panel(solar_panel_key)
    date_key                        TIMESTAMP     NOT NULL,   -- REFERENCES dim_date(date_key)
    
    solar_panel_id                  INT           NOT NULL,
    generation_power_wh             FLOAT         NOT NULL 
)
USING iceberg
PARTITIONED BY (MONTH(date_key), solar_panel_id)
"""


create_wh_dim_date_schema = """
CREATE TABLE SolarX_WH.dim_date
(
    date_key            TIMESTAMP  NOT NULL,
    year                SMALLINT   NOT NULL,
    quarter             SMALLINT   NOT NULL,
    month               SMALLINT   NOT NULL,
    week                SMALLINT   NOT NULL,
    day                 SMALLINT   NOT NULL,
    hour                SMALLINT   NOT NULL,
    minute              SMALLINT   NOT NULL,
    is_weekend          BOOLEAN    NOT NULL
)
USING iceberg
PARTITIONED BY (month, minute)
"""