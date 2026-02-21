import snowflake.connector
from config.settings import SNOWFLAKE_CONFIG

class SnowflakeClient:
    def __init__(self):
        self.config = SNOWFLAKE_CONFIG
        self.conn = snowflake.connector.connect(**self.config)

    def execute(self, query: str):
        with self.conn.cursor() as cursor:
            cursor.execute(query)

    def close(self):
        self.conn.close()
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()