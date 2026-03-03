from .snowflake_client import SnowflakeClient
import pandas as pd
from datetime import date

def get_records(schema: str, query: str, save_data: bool = True) -> pd.DataFrame:
    try:
        with SnowflakeClient(schema) as client:
            data = client.fetch_all(query)
        df = pd.DataFrame(data)
        current_date = date.today().strftime("%Y_%m_%d")
        if save_data:
            df.to_csv(f"data/{current_date}.csv", index=False)
        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()