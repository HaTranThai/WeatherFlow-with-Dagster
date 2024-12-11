from .WeatherDataExtractor import WeatherDataExtractor

# Class con cho dữ liệu thời tiết hiện tại
class CurrentData(WeatherDataExtractor):
    def extract_current_weather(self, city, lang="vi"):
        """
        Lấy dữ liệu thời tiết hiện tại.
        """
        params = {
            "id": city,
            "appid": self.api_key,
            "lang": lang
        }
        return self.fetch_data("", params)