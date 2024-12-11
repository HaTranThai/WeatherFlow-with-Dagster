from .WeatherDataExtractor import WeatherDataExtractor

class AirPollutionData(WeatherDataExtractor):
    def extract_air_pollution(self, lat, lon):
        """
        Lấy dữ liệu ô nhiễm không khí.
        """
        params = {
            "lat": lat,
            "lon": lon,
            "appid": self.api_key,
        }
        return self.fetch_data("", params)