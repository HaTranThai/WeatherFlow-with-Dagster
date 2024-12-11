import json
import requests
from confluent_kafka import Producer

class WeatherDataExtractor:
    def __init__(self, api_key, base_url, servers):
        self.api_key = api_key
        self.base_url = base_url
        self.servers = servers
        self.producer = self.configure_kafka()

    def fetch_data(self, endpoint, params):
        """
        Phương thức chung để gửi yêu cầu tới API.
        """
        url = f"{self.base_url}"  # Kết hợp URL cơ sở với endpoint
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error fetching data: {e}")
            return None

    def configure_kafka(self):
        """
        Cấu hình Kafka Producer.
        """
        settings = {
            'bootstrap.servers': ','.join(self.servers),
            'client.id': 'weather_producer'
        }
        return Producer(settings)

    def send_to_kafka(self, topic, data):
        """
        Gửi dữ liệu tới Kafka.
        """
        try:
            self.producer.produce(topic, json.dumps(data), callback=self.delivery_report)
            self.producer.flush()  # Đảm bảo dữ liệu được gửi ngay lập tức
        except Exception as e:
            print(f"Failed to send message to Kafka: {e}")

    @staticmethod
    def delivery_report(err, msg):
        """
        Báo cáo kết quả gửi dữ liệu Kafka.
        """
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")