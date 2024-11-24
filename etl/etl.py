from sqlalchemy import create_engine, text
import pandas as pd
import os

# Get credentials
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
CSV_NAME = os.getenv("CSV_NAME")

# Database configuration
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

# Define a dictionary to map columns to extract
coord_mappings = {
    'origin_coord': ['origin_longitude', 'origin_latitude'],
    'destination_coord': ['destin_longitude', 'destin_latitude']
}

# Reading raw CSV
file_path = f'./data/{CSV_NAME}.csv'
raw_df = pd.read_csv(file_path, sep=',')

raw_df['datetime'] = pd.to_datetime(raw_df['datetime'], errors='coerce')

# Iterate over the dictionary to perform extraction
for coord_col, new_cols in coord_mappings.items():
    raw_df[new_cols] = raw_df[coord_col].str.extract(r'POINT \(([-\d.]+) ([-\d.]+)\)')

# Handle dimensions update

## DIM_DATASOURCES
datasource_df_raw = raw_df["datasource"].drop_duplicates()
datasource_df_raw = datasource_df_raw.rename("datasource").to_frame()

# Updating dim_datasource
def insert_into_dim_datasource(engine, df):
    with engine.connect() as conn:
        for _, row in df.iterrows():
            datasource_value = row["datasource"]

            # Check if 'source_name' already exists in the database
            check_query = text("""  
                SELECT COUNT(*) FROM dim_datasource WHERE source_name = :source_name
            """)
            result = conn.execute(check_query, {"source_name": datasource_value}).fetchone()

            if result[0] == 0:
                try:
                    insert_query = text("""
                        INSERT INTO dim_datasource (source_name)
                        VALUES (:source_name)
                        RETURNING id
                    """)
                    result = conn.execute(insert_query, {"source_name": datasource_value})

                    conn.commit()

                    print(f"Registry '{datasource_value}' inserted.")
                except Exception as e:
                    print(f"Error during query execution to '{datasource_value}': {e}")
            else:
                pass

insert_into_dim_datasource(engine, datasource_df_raw)

## DIM_LOCATION
### Looking to origin coordinates
location_origin_df = raw_df[["origin_longitude", "origin_latitude", "region"]].drop_duplicates()

# Updating dim_location (origin)
def insert_into_dim_location(engine, df):
    with engine.connect() as conn:
        for _, row in df.iterrows():
            longitude_value = row["origin_longitude"]
            latitude_value = row["origin_latitude"]
            region_value = row["region"]

            # Check if the location (longitude, latitude) already exists in the database
            check_query = text("""
                SELECT COUNT(*) FROM dim_location 
                WHERE longitude = :longitude AND latitude = :latitude
            """)
            result = conn.execute(check_query, {
                "longitude": longitude_value,
                "latitude": latitude_value
            }).fetchone()

            if result[0] == 0:
                # If the location does not exist, insert it
                try:
                    insert_query = text("""
                        INSERT INTO dim_location (longitude, latitude, region)
                        VALUES (:longitude, :latitude, :region)
                    """)
                    result = conn.execute(insert_query, {
                        "longitude": longitude_value,
                        "latitude": latitude_value,
                        "region": region_value
                    })

                    # Commit the transaction (not necessary for the current context, SQLAlchemy will commit automatically)
                    conn.commit()
                except Exception as e:
                    print(f"Error during query execution for {longitude_value}, {latitude_value}, {region_value}: {e}")
            else:
                pass

insert_into_dim_location(engine, location_origin_df)

### Looking to destination coordinates
location_destination_df = raw_df[["destin_longitude", "destin_latitude", "region"]].drop_duplicates()

def verify_destination_dim_location(engine, df):
    with engine.connect() as conn:
        for _, row in df.iterrows():
            longitude_value = row["destin_longitude"]
            latitude_value = row["destin_latitude"]
            region_value = row["region"]

            # Check if the location (longitude, latitude) already exists in the database
            check_query = text("""
                SELECT COUNT(*) FROM dim_location 
                WHERE longitude = :longitude AND latitude = :latitude
            """)
            result = conn.execute(check_query, {
                "longitude": longitude_value,
                "latitude": latitude_value
            }).fetchone()

            if result[0] == 0:
                # If the location does not exist, insert it
                try:
                    insert_query = text("""
                        INSERT INTO dim_location (longitude, latitude, region)
                        VALUES (:longitude, :latitude, :region)
                    """)
                    result = conn.execute(insert_query, {
                        "longitude": longitude_value,
                        "latitude": latitude_value,
                        "region": region_value
                    })

                    # Commit the transaction (not necessary for the current context, SQLAlchemy will commit automatically)
                    conn.commit()
                except Exception as e:
                    print(f"Error during query execution for {longitude_value}, {latitude_value}, {region_value}: {e}")
            else:
                pass

verify_destination_dim_location(engine, location_destination_df)

## FACT_TRIPS

# Query to fetch data from dim_location
location_query = """
    SELECT id AS location_id, latitude, longitude
    FROM dim_location;
"""

# Query to fetch data from dim_datasource
datasource_query = """
    SELECT id AS datasource_id, source_name
    FROM dim_datasource;
"""

# Execute the queries and store the results in separate DataFrames
dim_location_df = pd.read_sql(location_query, engine)
dim_datasource_df = pd.read_sql(datasource_query, engine)

final_df= raw_df.merge(dim_location_df, left_on=['origin_longitude','origin_latitude'], right_on=['longitude','latitude'], how='left').rename(columns={'location_id': 'location_orig_id'})
final_df = final_df.merge(dim_location_df, left_on=['destin_longitude','destin_latitude'], right_on=['longitude','latitude'], how='left').rename(columns={'location_id': 'location_dest_id'})
final_df = final_df.merge(dim_datasource_df, left_on='datasource', right_on='source_name', how='left')
final_df = final_df[["location_orig_id","location_dest_id", "datasource_id", "datetime"]]

final_df.to_sql(
    name='fact_trips',
    con=engine,
    schema='public',  
    if_exists='append',  
    index=False,  
)
print("Data inserted into fact_trips successfully!")