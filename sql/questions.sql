-- From the two most commonly appearing regions, which is the latest datasource?
WITH region_counts AS (
    -- Step 1: Count occurrences of regions in fact_trips (origin and destination combined)
    SELECT 
        l.region, 
        COUNT(*) AS trip_count
    FROM fact_trips f
    JOIN dim_location l ON f.location_orig_id = l.id OR f.location_dest_id = l.id
    GROUP BY l.region
    ORDER BY trip_count DESC
    LIMIT 2
),
latest_trips AS (
    -- Step 2: Filter trips for the two most common regions and find the latest datetime
    SELECT 
        f.datasource_id,
        f.datetime,
        l.region
    FROM fact_trips f
    JOIN dim_location l ON f.location_orig_id = l.id OR f.location_dest_id = l.id
    WHERE l.region IN (SELECT region FROM region_counts)
    ORDER BY f.datetime DESC
),
max_datetime AS (
    -- Step 3: Get the maximum datetime from the latest_trips CTE
    SELECT MAX(datetime) AS max_datetime
    FROM latest_trips
),
latest_datasource AS (
    -- Step 4: Find the latest datasource for each region, using max_datetime from latest_trips
    SELECT 
        l.region,
        f.datasource_id,
        date(f.datetime) as datetime
    FROM fact_trips f
    JOIN dim_location l ON f.location_orig_id = l.id OR f.location_dest_id = l.id
    WHERE l.region IN (SELECT region FROM region_counts)
    AND f.datetime = (SELECT max_datetime FROM max_datetime)
    limit 1
)
-- Step 5: Get the final result with datasource name
SELECT 
    ld.source_name AS latest_datasource,
    ld.id AS datasource_id,
    r.region,
    r.datetime AS latest_datetime
FROM latest_datasource r
JOIN dim_datasource ld ON r.datasource_id = ld.id;

-- What regions has the "cheap_mobile" datasource appeared in?

WITH cheap_mobile_datasource AS (
    -- Step 1: Get the datasource_id for the 'cheap_mobile' datasource
    SELECT id
    FROM dim_datasource
    WHERE source_name = 'cheap_mobile'
)
SELECT DISTINCT l.region
FROM fact_trips f
JOIN dim_location l ON f.location_orig_id = l.id OR f.location_dest_id = l.id
WHERE f.datasource_id = (SELECT id FROM cheap_mobile_datasource)
ORDER BY l.region;