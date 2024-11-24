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
                print(f"Registry '{datasource_value}' already exists.")

insert_into_dim_datasource(engine, datasource_df)
