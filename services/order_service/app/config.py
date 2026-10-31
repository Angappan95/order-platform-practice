import os
from functools import lru_cache


class Settings:
    def __init__(self):
        self.app_name = os.getenv("APP_NAME", "order-service")        
        self.environment = os.getenv("ENVIRONMENT", "local")        
        self.database_url = os.environ["DATABASE_URL"]        
        self.inventory_url = os.getenv("INVENTORY_URL", "http://inventory-service:8000")        
        self.payment_url = os.getenv("PAYMENT_URL", "http://payment-service:8000")        
        self.kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")

@lru_cache
def get_settings():
    return Settings()
