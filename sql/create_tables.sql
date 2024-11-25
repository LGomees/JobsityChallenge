----------------------------------------------------

CREATE TABLE dim_location (
    id SERIAL PRIMARY KEY,      
    region VARCHAR(100), 
    latitude VARCHAR(25),   
    longitude VARCHAR(25)   
);

CREATE INDEX idx_id ON dim_location (id);
CREATE INDEX idx_latitude_longitude ON dim_location (latitude, longitude);
CREATE INDEX idx_region ON dim_location (region);


----------------------------------------------------

CREATE TABLE dim_datasource (
    id SERIAL PRIMARY KEY,         
    source_name VARCHAR(255)      
);

CREATE INDEX idx_id ON dim_datasource (id);
CREATE INDEX idx_source_name ON dim_datasource (source_name);

----------------------------------------------------

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

CREATE INDEX idx_full_date ON dim_time (full_date);

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

----------------------------------------------------

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

CREATE INDEX idx_fact_trips_origin ON fact_trips (location_orig_id);

CREATE INDEX idx_fact_trips_dest ON fact_trips (location_dest_id);

CREATE INDEX idx_fact_trips_datasource ON fact_trips (datasource_id);