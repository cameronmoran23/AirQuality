import os
import requests

OPEN_AQ_API_KEY = os.getenv("OPEN_AQ_API_KEY")

class Extract:
    """
    A class to extract air quality data from OpenAQ API.
    """
    def fetch_air_quality_data(self, coordinates, radius=10000, limit=1000):
        """
        Fetch air quality data from OpenAQ API for a given city and parameter.
        
        Parameters:
        coordinates (str): The latitude and longitude of the location 
        radius (int): The radius in meters 
        limit (int): The maximum number of results
        
        Returns:
        dict: The JSON response from the API
        """
        base_url = "https://api.openaq.org/v3/locations"
        headers = {
            "X-API-Key": OPEN_AQ_API_KEY
        }
        params = {
            "coordinates": coordinates,
            "radius": radius,
            "limit": limit,
        }
        try: 
            response = requests.get(base_url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"A requests exception has occured: {e}\n Returning None")
            return None
    
        except Exception as e:
            print(f"A general exception has occured: {e}\n Returning None")
            return None
        

if __name__ == "__main__":
    coordinates = "34.0522,-118.2437"  # Coordinates for Los Angeles
    extractor = Extract()
    data = extractor.fetch_air_quality_data(coordinates)
    print(data)