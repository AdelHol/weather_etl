# Database Tables – Weather ETL Project

This document describes the structure of the PostgreSQL schema used by the ETL pipeline to collect and store weather data.

---

## Schema: `reporting_data`

---

### Table: `weather_current`

| Column          | Type               | Description                                |
|-----------------|--------------------|--------------------------------------------|
| id              | SERIAL PRIMARY KEY | Primary key                                |
| city            | VARCHAR(50)        | City name                                  |
| as_of           | TIMESTAMP          | Timestamp of the measurement               |
| temp_c          | DOUBLE PRECISION   | Temperature in °C                          |
| temp_f          | DOUBLE PRECISION   | Temperature in °F                          |
| is_day          | SMALLINT           | Day (1) / Night (0)                         |
| condition_text  | TEXT               | Text description of the weather condition  |
| condition_icon  | TEXT               | Weather condition icon                     |
| condition_code  | INTEGER            | Weather condition code                     |
| wind_mph        | DOUBLE PRECISION   | Wind speed in mph                          |
| wind_kph        | DOUBLE PRECISION   | Wind speed in kph                          |
| wind_degree     | INTEGER            | Wind direction in degrees                  |
| wind_dir        | VARCHAR(10)        | Wind direction abbreviation (e.g., N, S)   |
| pressure_mb     | DOUBLE PRECISION   | Pressure in millibars                      |
| pressure_in     | DOUBLE PRECISION   | Pressure in inches                         |
| precip_mm       | DOUBLE PRECISION   | Precipitation in millimeters               |
| precip_in       | DOUBLE PRECISION   | Precipitation in inches                    |
| humidity        | INTEGER            | Humidity (%)                               |
| cloud           | INTEGER            | Cloud cover (%)                            |
| feelslike_c     | DOUBLE PRECISION   | Feels-like temperature in °C               |
| feelslike_f     | DOUBLE PRECISION   | Feels-like temperature in °F               |
| windchill_c     | DOUBLE PRECISION   | Wind chill in °C                           |
| windchill_f     | DOUBLE PRECISION   | Wind chill in °F                           |
| heatindex_c     | DOUBLE PRECISION   | Heat index in °C                           |
| heatindex_f     | DOUBLE PRECISION   | Heat index in °F                           |
| dewpoint_c      | DOUBLE PRECISION   | Dew point in °C                            |
| dewpoint_f      | DOUBLE PRECISION   | Dew point in °F                            |
| vis_km          | DOUBLE PRECISION   | Visibility in kilometers                   |
| vis_miles       | DOUBLE PRECISION   | Visibility in miles                        |
| gust_mph        | DOUBLE PRECISION   | Wind gusts in mph                          |
| gust_kph        | DOUBLE PRECISION   | Wind gusts in kph                          |
| uv              | DOUBLE PRECISION   | UV index                                   |
| fetched_at      | TIMESTAMP          | Timestamp when the data was fetched        |

**Indexes:**
- Unique index: `(city, as_of)`

---

### Table: `weather_forecast`

| Column                 | Type               | Description                                |
|------------------------|--------------------|--------------------------------------------|
| id                     | SERIAL PRIMARY KEY | Primary key                                |
| city                   | VARCHAR(50)        | City name                                  |
| forecast_for           | TIMESTAMP          | Forecast timestamp                         |
| temp_c                 | DOUBLE PRECISION   | Temperature in °C                          |
| temp_f                 | DOUBLE PRECISION   | Temperature in °F                          |
| is_day                 | SMALLINT           | Day/Night indicator                        |
| condition_text         | TEXT               | Text description of the weather condition  |
| condition_icon         | TEXT               | Weather condition icon                     |
| condition_code         | INTEGER            | Weather condition code                     |
| wind_mph               | DOUBLE PRECISION   | Wind speed in mph                          |
| wind_kph               | DOUBLE PRECISION   | Wind speed in kph                          |
| wind_degree            | INTEGER            | Wind direction in degrees                  |
| wind_dir               | VARCHAR(10)        | Wind direction abbreviation (e.g., N, S)   |
| pressure_mb            | DOUBLE PRECISION   | Pressure in millibars                      |
| pressure_in            | DOUBLE PRECISION   | Pressure in inches                         |
| precip_mm              | DOUBLE PRECISION   | Precipitation in millimeters               |
| precip_in              | DOUBLE PRECISION   | Precipitation in inches                    |
| humidity               | INTEGER            | Humidity (%)                               |
| cloud                  | INTEGER            | Cloud cover (%)                            |
| feelslike_c            | DOUBLE PRECISION   | Feels-like temperature in °C               |
| feelslike_f            | DOUBLE PRECISION   | Feels-like temperature in °F               |
| windchill_c            | DOUBLE PRECISION   | Wind chill in °C                           |
| windchill_f            | DOUBLE PRECISION   | Wind chill in °F                           |
| heatindex_c            | DOUBLE PRECISION   | Heat index in °C                           |
| heatindex_f            | DOUBLE PRECISION   | Heat index in °F                           |
| dewpoint_c             | DOUBLE PRECISION   | Dew point in °C                            |
| dewpoint_f             | DOUBLE PRECISION   | Dew point in °F                            |
| will_it_rain           | SMALLINT           | Will it rain? (0 = No, 1 = Yes)            |
| chance_of_rain         | SMALLINT           | Probability of rain (%)                    |
| vis_km                 | DOUBLE PRECISION   | Visibility in kilometers                   |
| vis_miles              | DOUBLE PRECISION   | Visibility in miles                        |
| gust_mph               | DOUBLE PRECISION   | Wind gusts in mph                          |
| gust_kph               | DOUBLE PRECISION   | Wind gusts in kph                          |
| uv                     | DOUBLE PRECISION   | UV index                                   |
| prediction_generated_at| TIMESTAMP          | Timestamp when prediction was generated    |
| fetched_at             | TIMESTAMP          | Timestamp when the data was fetched        |

**Indexes:**
- Unique index: `(city, forecast_for, prediction_generated_at)`




