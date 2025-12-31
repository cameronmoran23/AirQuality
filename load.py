from sqlalchemy import create_engine
from sqlalchemy import text
from extract import Extract
from transform import Transform
import os

class Load:
    """
    A class to load transformed data into a PostgreSQL database.
    """
    def __init__(self, user, password, host, port, db):
        """
        Initialize the Load class with PostgreSQL connection parameters.
        
        Parameters:
        user (str): PostgreSQL username
        password (str): PostgreSQL password
        host (str): PostgreSQL host
        port (str): PostgreSQL port
        db (str): PostgreSQL database name
        """
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.db = db
        self.engine = create_engine(f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}")

    def write_data(self, df, table_name):
        """
        Write a DataFrame to a PostgreSQL table.
        
        Parameters:
        df (pd.DataFrame): The DataFrame to write to the database
        table_name (str): The name of the table in the database
        """
        try:
            df.to_sql(table_name, self.engine, if_exists='replace', index=False)
            print(f"Data written to table {table_name} successfully.")
        except Exception as e:
            print(f"Error writing data to table {table_name}: {e}")

if __name__ == "__main__":
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    loader = Load("airquality_user", POSTGRES_PASSWORD, "localhost", "5432", "airquality_db")
    extractor = Extract()
    coordinates = "34.0522,-118.2437"  # Coordinates for Los Angeles
    data = extractor.fetch_air_quality_data(coordinates)
    if data:
        transformer = Transform()
        df = transformer.transform_data(data)
        loader.write_data(df, "air_quality_data")
