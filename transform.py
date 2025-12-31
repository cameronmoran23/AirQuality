import pandas as pd
from sklearn.impute import SimpleImputer
import json

class Transform:
    """
    A class to transform and validate air quality data.
    """
    def main(self, raw_data: dict) -> pd.DataFrame:
        """
        Accepts data in response.json() format and transforms it into a pandas DataFrame. Validation also occurs here.

        Parameters:
        raw_data (dict): The JSON response from the API

        Returns:
        pd.DataFrame: The transformed and validated data
        """
        if self.validate_data(raw_data):
            return self.transform_data(raw_data)
        else:
            print("Unable to transform data.")
            return pd.DataFrame()  # Return empty df
    
    def validate_data(self, raw_data) -> bool:
        """
        Validates the raw data

        Parameters:
        raw_data (dict): The JSON response from the API

        Returns:
        bool: True if data is valid, False otherwise
        """
        if raw_data is None:
            print("Data is None.")
            return False
        
        if "meta" not in raw_data or "results" not in raw_data:
            print("Missing required keys in data.")
            return False
        
        return True

    def transform_data(self, raw_data: dict) -> pd.DataFrame:
        """
        Transforms the raw data into a pandas DataFrame and fills missing values.

        Parameters:
        raw_data (dict): The JSON response from the API

        Returns:
        pd.DataFrame: The transformed data
        """
        df = pd.DataFrame(raw_data["results"])
        df = self.fill_df(df)
        # drop any remaining rows with null values
        df = df.dropna()
        df = self.convert_json_columns(df)
        return df
    
    def convert_json_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Converts a column with JSON strings into proper JSON objects

        Parameters:
        df (pd.DataFrame): The DataFrame containing the column to convert
        column_name (str): The name of the column to convert

        Returns:
        pd.DataFrame: The DataFrame with the converted column
        """

        json_columns = [
            "coordinates",
            "datetimeFirst",
            "datetimeLast",
            "sensors",
            "instruments",
            "provider",
            "owner",
            "country",
            "licenses",
            "bounds"
        ]

        for col in json_columns:
            df[col] = df[col].apply(json.dumps)
        return df

    def fill_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fills missing values in the DataFrame using the methods defined below

        Parameters:
        df (pd.DataFrame): The DataFrame with potential missing values

        Returns:
        pd.DataFrame: The DataFrame with missing values filled
        """
        df = self.fill_ids(
        self.fill_name(
        self.fill_locality(
        self.fill_timezone(
        self.fill_country(
        self.fill_owner(
        self.fill_provider(
        self.fill_isMobile(
        self.fill_isMonitor(
        self.fill_instruments(
        self.fill_sensors(
        self.fill_coordinates(
        self.fill_licenses(
        self.fill_bounds(
        self.fill_distance(
        self.fill_datetimeFirst(
        self.fill_datetimeLast(df)
        ))))))))))))))))
        return df
    
    def fill_ids(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fills missing IDs with unique integers starting from the max existing ID + 1

        Parameters:
        df (pd.DataFrame): The DataFrame with potential missing IDs

        Returns:
        pd.DataFrame: The DataFrame with missing IDs filled
        """
        curr_id = df["id"].max() + 1
        for index, row in df.iterrows():
            if pd.isna(row["id"]):
                df.at[index, "id"] = curr_id
                curr_id += 1
        df["id"] = df["id"].astype(int)
        return df
    
    def fill_name(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fills missing names with "Unknown"

        Parameters:
        df (pd.DataFrame): The DataFrame with potential missing names

        Returns:
        pd.DataFrame: The DataFrame with missing names filled
        """
        df["name"] = df["name"].fillna("Unknown")
        df["name"] = df["name"].astype(str)
        return df
    
    def fill_locality(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fills missing locality values with "Unknown"

        Parameters:
        df (pd.DataFrame): The DataFrame with potential missing locality values

        Returns:
        pd.DataFrame: The DataFrame with missing locality values filled
        """
        df["locality"] = df["locality"].fillna("Unknown")
        df["locality"] = df["locality"].astype(str)
        return df

    def fill_timezone(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fills missing timezone values using the most frequent timezone

        Parameters:
        df (pd.DataFrame): The DataFrame with potential missing timezone values

        Returns:
        pd.DataFrame: The DataFrame with missing timezone values filled
        """
        mode_imputer = SimpleImputer(strategy="most_frequent")
        df["timezone"] = mode_imputer.fit_transform(df[["timezone"]]).flatten()
        df["timezone"] = df["timezone"].astype(str)
        return df

    def fill_country(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fills missing country values using forward fill and backward fill

        Parameters:
        df (pd.DataFrame): The DataFrame with potential missing country values

        Returns:
        pd.DataFrame: The DataFrame with missing country values filled
        """
        df["country"] = df["country"].ffill().bfill()
        df["country"] = df["country"].astype(object)
        return df

    def fill_owner(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fills missing owner values using forward fill and backward fill

        Parameters:
        df (pd.DataFrame): The DataFrame with potential missing owner values

        Returns:
        pd.DataFrame: The DataFrame with missing owner values filled
        """
        df["owner"] = df["owner"].ffill().bfill()
        df["owner"] = df["owner"].astype(object)
        return df

    def fill_provider(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fills missing provider values using forward fill and backward fill

        Parameters:
        df (pd.DataFrame): The DataFrame with potential missing provider values

        Returns:
        pd.DataFrame: The DataFrame with missing provider values filled
        """
        df["provider"] = df["provider"].ffill().bfill()
        df["provider"] = df["provider"].astype(object)
        return df
    
    def fill_isMobile(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fills missing isMobile values with False

        Parameters:
        df (pd.DataFrame): The DataFrame with potential missing isMobile values

        Returns:
        pd.DataFrame: The DataFrame with missing isMobile values filled
        """
        df["isMobile"] = df["isMobile"].fillna(False)
        df["isMobile"] = df["isMobile"].astype(bool)
        return df
    
    def fill_isMonitor(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fills missing isMonitor values with False

        Parameters:
        df (pd.DataFrame): The DataFrame with potential missing isMonitor values

        Returns:
        pd.DataFrame: The DataFrame with missing isMonitor values filled
        """
        df["isMonitor"] = df["isMonitor"].fillna(False)
        df["isMonitor"] = df["isMonitor"].astype(bool)
        return df
    
    def fill_instruments(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fills missing instruments values using forward fill and backward fill

        Parameters:
        df (pd.DataFrame): The DataFrame with potential missing instruments values

        Returns:
        pd.DataFrame: The DataFrame with missing instruments values filled
        """
        df["instruments"] = df["instruments"].ffill().bfill()
        df["instruments"] = df["instruments"].astype(object)
        return df

    def fill_sensors(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fills missing sensors values using forward fill and backward fill

        Parameters:
        df (pd.DataFrame): The DataFrame with potential missing sensors values

        Returns:
        pd.DataFrame: The DataFrame with missing sensors values filled
        """
        df["sensors"] = df["sensors"].ffill().bfill()
        df["sensors"] = df["sensors"].astype(object)
        return df
    
    def fill_coordinates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fills missing coordinates values using forward fill and backward fill

        Parameters:
        df (pd.DataFrame): The DataFrame with potential missing coordinates values

        Returns:
        pd.DataFrame: The DataFrame with missing coordinates values filled
        """
        df["coordinates"] = df["coordinates"].ffill().bfill()
        df["coordinates"] = df["coordinates"].astype(object)
        return df

    def fill_licenses(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fills missing licenses values using forward fill and backward fill

        Parameters:
        df (pd.DataFrame): The DataFrame with potential missing licenses values

        Returns:
        pd.DataFrame: The DataFrame with missing licenses values filled
        """
        df["licenses"] = df["licenses"].ffill().bfill()
        df["licenses"] = df["licenses"].astype(object)
        return df
    
    def fill_bounds(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fills missing bounds values using forward fill and backward fill

        Parameters:
        df (pd.DataFrame): The DataFrame with potential missing bounds values

        Returns:
        pd.DataFrame: The DataFrame with missing bounds values filled
        """
        df["bounds"] = df["bounds"].ffill().bfill()
        df["bounds"] = df["bounds"].astype(object)
        return df
    
    def fill_distance(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fills missing distance values using the mean distance

        Parameters:
        df (pd.DataFrame): The DataFrame with potential missing distance values

        Returns:
        pd.DataFrame: The DataFrame with missing distance values filled
        """
        mean_imputer = SimpleImputer(strategy="mean")
        df["distance"] = mean_imputer.fit_transform(df[["distance"]])
        df["distance"] = df["distance"].astype(float)
        return df
    
    def fill_datetimeFirst(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fills missing datetimeFirst values using forward fill and backward fill

        Parameters:
        df (pd.DataFrame): The DataFrame with potential missing datetimeFirst values

        Returns:
        pd.DataFrame: The DataFrame with missing datetimeFirst values filled
        """
        df["datetimeFirst"] = df["datetimeFirst"].ffill().bfill()
        df["datetimeFirst"] = df["datetimeFirst"].astype(object)
        return df
    
    def fill_datetimeLast(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fills missing datetimeLast values using forward fill and backward fill

        Parameters:
        df (pd.DataFrame): The DataFrame with potential missing datetimeLast values

        Returns:
        pd.DataFrame: The DataFrame with missing datetimeLast values filled
        """
        df["datetimeLast"] = df["datetimeLast"].ffill().bfill()
        df["datetimeLast"] = df["datetimeLast"].astype(object)
        return df
    
if __name__ == "__main__":
    from extract import Extract

    coordinates = "34.0522,-118.2437"  # Coordinates for Los Angeles
    extractor = Extract()
    raw_data = extractor.fetch_air_quality_data(coordinates)

    transformer = Transform()
    transformed_data = transformer.main(raw_data)
    transformed_data.to_csv("transformed_air_quality_data.csv", index=False)
        