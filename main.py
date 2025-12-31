from extract import Extract
from transform import Transform
from load import Load
import os

def main(coordinates="00.0000,0.0000"):
    """
    Main driver method to run the ETL process
    """
    # Initialize classes
    extractor = Extract()
    transformer = Transform()
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    loader = Load("airquality_user", POSTGRES_PASSWORD, "localhost", "5432", "airquality_db")

    # Extract
    raw_data = extractor.fetch_air_quality_data(coordinates)

    # Transform
    transformed_data = transformer.transform_data(raw_data)

    # Load
    loader.write_data(transformed_data, "air_quality_data")
    print("ETL process completed successfully.")

if __name__ == "__main__":
    coords = "34.0522,-118.2437"  # Coordinates for Los Angeles
    main(coords)