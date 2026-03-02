import snowflake.connector
from config.settings import SNOWFLAKE_CONFIG

class SnowflakeClient:
    def __init__(self, schema: str | None = None):
        self.config = SNOWFLAKE_CONFIG.copy()
        if schema:
            self.config['schema'] = schema
        self.conn = snowflake.connector.connect(**self.config)

    def execute(self, query: str, fetch: bool = False):
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            if fetch:
                return cursor.fetchall()
        return None
    
    def fetch_all(self, query: str):
        return self.execute(query, fetch=True) 

    def close(self):
        self.conn.close()
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()