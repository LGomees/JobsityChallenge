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

# Reading raw CSV
file_path = f'./data/{CSV_NAME}.csv'
raw_df = pd.read_csv(file_path, sep=',')

# Handle dimensions update

## DIM_DATASOURCES
datasource_df = raw_df["datasource"].drop_duplicates()
datasource_df = datasource_df.rename("datasource").to_frame()

# Updating dim_datasource
# def insert_into_dim_datasource(engine, df):
#     with engine.connect() as conn:
#         for _, row in df.iterrows():
#             datasource_value = row["datasource"]

#             # Check if 'source_name' already exists in the database
#             check_query = text("""  
#                 SELECT COUNT(*) FROM dim_datasource WHERE source_name = :source_name
#             """)
#             result = conn.execute(check_query, {"source_name": datasource_value}).fetchone()

#             if result[0] == 0:
#                 try:
#                     insert_query = text("""
#                         INSERT INTO dim_datasource (source_name)
#                         VALUES (:source_name)
#                         RETURNING id
#                     """)
#                     result = conn.execute(insert_query, {"source_name": datasource_value})

#                     conn.commit()

#                     print(f"Registry '{datasource_value}' inserted.")
#                 except Exception as e:
#                     print(f"Error during query execution to '{datasource_value}': {e}")
#             else:
#                 print(f"Registry '{datasource_value}' already exists.")

# insert_into_dim_datasource(engine, datasource_df)

# ## DIM_LOCATION
# ### Looking to origin coordinates
# location_origin_df = raw_df[["origin_coord", "region"]].drop_duplicates()

# # Separating long and lat to be inserted in the database
# location_origin_df[['longitude', 'latitude']] = location_origin_df['origin_coord'].str.extract(r'POINT \(([-\d.]+) ([-\d.]+)\)')

# # Updating dim_location (origin)
# def insert_into_dim_location(engine, df):
#     with engine.connect() as conn:
#         for _, row in df.iterrows():
#             longitude_value = row["longitude"]
#             latitude_value = row["latitude"]
#             region_value = row["region"]

#             # Check if the location (longitude, latitude) already exists in the database
#             check_query = text("""
#                 SELECT COUNT(*) FROM dim_location 
#                 WHERE longitude = :longitude AND latitude = :latitude
#             """)
#             result = conn.execute(check_query, {
#                 "longitude": longitude_value,
#                 "latitude": latitude_value
#             }).fetchone()

#             if result[0] == 0:
#                 # If the location does not exist, insert it
#                 try:
#                     insert_query = text("""
#                         INSERT INTO dim_location (longitude, latitude, region)
#                         VALUES (:longitude, :latitude, :region)
#                     """)
#                     result = conn.execute(insert_query, {
#                         "longitude": longitude_value,
#                         "latitude": latitude_value,
#                         "region": region_value
#                     })

#                     # Commit the transaction (not necessary for the current context, SQLAlchemy will commit automatically)
#                     conn.commit()
#                 except Exception as e:
#                     print(f"Error during query execution for {longitude_value}, {latitude_value}, {region_value}: {e}")
#             else:
#                 # If the location already exists, skip insertion
#                 print(f"Registry with Longitude {longitude_value}, Latitude {latitude_value}, and Region '{region_value}' already exists.")

# insert_into_dim_location(engine, location_origin_df)

### Looking to destination coordinates
location_destination_df = raw_df[["destination_coord", "region"]].drop_duplicates()

# Separating long and lat to be inserted in the database
location_destination_df[['longitude', 'latitude']] = location_destination_df['destination_coord'].str.extract(r'POINT \(([-\d.]+) ([-\d.]+)\)')

def verify_destination_dim_location(engine, df):
    with engine.connect() as conn:
        for _, row in df.iterrows():
            longitude_value = row["longitude"]
            latitude_value = row["latitude"]
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
                # If the location already exists, skip insertion
                print(f"Registry with Longitude {longitude_value}, Latitude {latitude_value}, and Region '{region_value}' already exists.")

verify_destination_dim_location(engine, location_destination_df)
