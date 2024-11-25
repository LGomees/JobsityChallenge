# Jobsity Challenge


## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Technologies Used](#technologies-used)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Folder Structure](#folder-structure)

## Overview

This project aims to build an automatic process to ingest data on demand. The data represents trips made by different vehicles and includes a city, a point of origin and a destination.
The flow processes raw trips data using REST API, extracts information about locations, data sources, and timestamp, and loads it into a PostgreSQL database using a star schema structure. The pipeline ensures proper management of dimensions and fact tables while optimizing queries.

## Features

- Extract and parse raw trip data.
- Manage dimensions like location and datasource.
- Populate a fact table (fact_trips) with processed data.
- Efficient handling of location coordinates (latitude, longitude).
- Flexible and reusable code structure.

## Technologies Used

- **Python**: Data processing and ETL.
- **Pandas**: Data manipulation.
- **PostgreSQL**: Database storage.
- **SQLAlchemy**: Database connection and ORM.
- **Docker**: Containerized development environment.
- **FastAPI**: Web framework for building APIs with Python, designed for high performance and easy integration with databases and external services.
- **Uvicorn**: ASGI server for serving FastAPI applications, known for its speed and support for asynchronous programming.

## Prerequisites

Before setting up the project, ensure you have the following installed:

- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)
- [Git](https://git-scm.com/)

## Installation

Follow these steps to set up the project locally:

1. **Clone the Repository:**

    ```bash
    git clone https://github.com/your-username/JobsityChallenge.git
    cd JobsityChallenge
    ```

2. **Set Up Environment Variables:**

    
    Thinking about greater code security, an .env file was created to deal with passwords and other credentials.
    
    Configure the `config.env` file in the root directory with the following variables:

    ```env
    DB_USER=jobuser
    DB_PASSWORD=jobpass
    DB_HOST=db
    DB_PORT=5432
    DB_NAME=trips
    ```

3. **Build and Run Docker Containers:**

    Ensure Docker is running, then execute:

    ```bash
    docker-compose up --build
    ```

    This will start the PostgreSQL database and the application container. Also, all the requirements will automaticaly be installed on the application container.

5. **Prepare the Database:**

    To deal with a large volume of data, the Star Schema approach was adopted. It is designed to optimize data retrieval for reporting and analysis. This model works with a fact table (main) and several other tables dealing with the dimensions that encompass the data made available.

    The assumptions made were that there are no dimensions to handle the existing data, except for the time dimension. This dimension needed to be populated manually so that the correct treatments could be executed. 

    The other dimensions needed to be created but there is a step to feed them based on the data received. In other words, the premise assumed at this point is that there may be several other options to be identified in the data and which must be treated.

    To start creating this tables, access the database using this command:

    ```bash
    docker exec -it jobsitychallenge-db-1 bash
    ```

    Once accessed, you will enter the following commands:

    - **DIM_LOCATION**

        - Create the dim_location and indexes:

    ```bash
    CREATE TABLE dim_location (
        id SERIAL PRIMARY KEY,      
        region VARCHAR(100), 
        latitude VARCHAR(25),   
        longitude VARCHAR(25)   
    );
    ```

    ```bash
    CREATE INDEX idx_id ON dim_location (id);
    CREATE INDEX idx_latitude_longitude ON dim_location (latitude, longitude);
    CREATE INDEX idx_region ON dim_location (region);
    ```

    - **DIM_DATASOURCE**

        - Create the dim_datasource and indexes:

    ```bash
    CREATE TABLE dim_datasource (
        id SERIAL PRIMARY KEY,         
        source_name VARCHAR(255)      
    );
    ```
    

    ```bash
    CREATE INDEX idx_id ON dim_datasource (id);
    CREATE INDEX idx_source_name ON dim_datasource (source_name);
    ```

    - **DIM_TIME**

        - Create the dim_time, indexes and populate the table using an already existing script (2010 - 2024):

    ```bash
    CREATE TABLE dim_time (
        time_id SERIAL PRIMARY KEY,        
        full_date DATE NOT NULL,           
        day INT NOT NULL,                  
        month INT NOT NULL,                
        year INT NOT NULL,                 
        quarter INT NOT NULL,              
        week_of_year INT NOT NULL,         
        day_of_week INT NOT NULL,          
        day_name VARCHAR(20),              
        month_name VARCHAR(20),            
        quarter_name VARCHAR(20),          
        fiscal_year INT,                   
        fiscal_quarter INT                 
    );
    ```

    ```bash
    CREATE INDEX idx_full_date ON dim_time (full_date);
    ```

    ```bash
    WITH RECURSIVE date_range AS (
    SELECT '2010-01-01'::DATE AS date  
    UNION ALL
    SELECT (date + INTERVAL '1 day')::DATE  
    FROM date_range
    WHERE date + INTERVAL '1 day' <= '2024-12-31'  
    )
    INSERT INTO dim_time (full_date, day, month, year, quarter, week_of_year, day_of_week, day_name, month_name, quarter_name)
    SELECT
        date,
        EXTRACT(DAY FROM date) AS day,
        EXTRACT(MONTH FROM date) AS month,
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(QUARTER FROM date) AS quarter,
        EXTRACT(WEEK FROM date) AS week_of_year,
        EXTRACT(DOW FROM date) AS day_of_week,
        TO_CHAR(date, 'Day') AS day_name,
        TO_CHAR(date, 'Month') AS month_name,
        CASE
            WHEN EXTRACT(QUARTER FROM date) = 1 THEN 'Q1'
            WHEN EXTRACT(QUARTER FROM date) = 2 THEN 'Q2'
            WHEN EXTRACT(QUARTER FROM date) = 3 THEN 'Q3'
            WHEN EXTRACT(QUARTER FROM date) = 4 THEN 'Q4'
        END AS quarter_name
    FROM date_range;
    ```

    - **FACT_TRIPS**

        - Create the fact_trips and indexes:

    ```bash
    CREATE TABLE fact_trips(
        id SERIAL PRIMARY KEY,               
        location_orig_id INT NOT NULL,            
        location_dest_id INT NOT NULL,            
        datasource_id INT NOT NULL,          
        datetime TIMESTAMP,               
        FOREIGN KEY (location_orig_id) REFERENCES dim_location(id) ON DELETE CASCADE,
        FOREIGN KEY (location_dest_id) REFERENCES dim_location(id) ON DELETE CASCADE,
        FOREIGN KEY (datasource_id) REFERENCES dim_datasource(id) ON DELETE cascade
    );
    ```

    ```bash
    CREATE INDEX idx_fact_trips_origin ON fact_trips (location_orig_id);
    CREATE INDEX idx_fact_trips_dest ON fact_trips (location_dest_id);
    CREATE INDEX idx_fact_trips_datasource ON fact_trips (datasource_id);
    ```

## Usage

1. **Access endpoints page**

    If Docker is already running through the previous steps, you can access this url to see all developed endpoints:

    ```bash
    http://localhost:8000/docs
    ```

2. **Run the ETL Pipeline**

    To create the API, FastAPI was used in conjunction with Uvicorn. FastAPI is a modern, high-performance web framework for building APIs with Python. It is specifically designed to be fast and easy to use. Using Uvicorn with FastAPI is a common and highly recommended combination for building fast, scalable web applications. It is an efficient way to notify the user of the process status using WebSockets.

    The endpoint called **run-ingestion** is responsible for receiving a CSV file containing the data and then executing the entire ETL script to transform the data up to the stage of insertion into the database.

    Going to the docs page, we can go to the endpoint and click on it. When clicking, we can see the “Try it out” button, which will open a field for inserting the file.

    After inserting the file, clicking the “Execute” button will launch the ETL script and perform both ingestion and storage of data.


3. **Results:**

    Check if the process worked by accessing the database again using the following command:

    ```bash
    docker exec -it jobsitychallenge-db-1 bash
    ```

    and then looking into the schema "trips":


    ```bash
    psql -U jobuser -d trips
    ```

    To see if the data was loaded, run the following query :

    ```bash
    SELECT * FROM fact_trips;
    ```


## Folder Structure

```
.
├── data/                # Directory for raw data files
├── etl/                 # ETL scripts
├── sql/                 # SQL scripts for schema creation
├── requirements.txt     # Python dependencies
├── docker-compose.yml   # Docker Compose configuration
├── Dockerfile           # Docker configuration
├── config.env           # Environment variables
├── README.md            # Project documentation
└── app.py               # API routes creation file
```
