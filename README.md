# Delta Green

## Assignment

### Part 1: Weather Data Collection

Create an **ETL pipeline** using a job orchestration tool (e.g. **Dagster**, **Prefect**, or another of your choice).

**Goal:**  
The data will serve a data analyst for the following purposes:
- To create a **dashboard** comparing forecasts to actual values.
- As **input for further calculations** using the latest version of the forecast at any given time.

**Source:**  
Use the **API from [WeatherAPI.com](https://www.weatherapi.com/)**.  
Register with your email address to access the API.  
A **14-day trial** will start upon registration and will automatically switch to the **free plan** after expiration.

**Frequency:**  
Download current weather data and a **48-hour forecast** every **15 minutes**.

**Locations:**  
- Prague  
- London

**Data Storage:**  
Store the data in a suitable **database**, ideally **PostgreSQL running in Docker**.  
There is no need to implement migrations — it's sufficient to create the tables once using a `.sql` file.

**Data Availability:**  
The data should be available for the purposes described above with **minimal latency**.

---

### Part 2: Data Loading

Create a **Jupyter Notebook** that loads the data from the database with the **latest forecast version for each hour**.

---

## Solution

The project is divided into several components:

### 1. ETL Pipeline (Dagster)

- Code is located in the [`weather_etl/`](./weather_etl) directory
- It runs on a regular schedule and:
  - Downloads data from WeatherAPI (current and forecast)
  - Stores it in a PostgreSQL database (including 15-minute backfilling)
  - See [`weather_etl/README.md`](./weather_etl/README.md)

### 2. Database

- Uses **PostgreSQL** (e.g. via Docker)
- Tables are created using [`sql/create_schema_and_tables.sql`](./weather_etl/sql/create_schema_and_tables.sql)
- Documentation: [`doc/TABLES.md`](./doc/TABLES.md)

### 3. Data Analysis

- Notebook [`notebooks/comparison.ipynb`](./notebooks/comparison.ipynb) performs:
  - Loading of actual and forecasted data
  - Computation of errors (MAE, RMSE, standard deviation)
  - Comparison of forecast to reality
  - Visualization using `matplotlib`



