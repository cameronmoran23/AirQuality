from extract import Extract
from transform import Transform
from load import Load

class Main:
    def __init__(self):
        self.extractor = Extract()
        self.transformer = Transform()
        self.loader = Load()
        
    def run(self, coordinates):
        # Extract
        raw_data = self.extractor.fetch_air_quality_data(coordinates)

        # Transform
        transformed_data = self.transformer.transform_data(raw_data)

        # Load
        self.loader.load_data(transformed_data)