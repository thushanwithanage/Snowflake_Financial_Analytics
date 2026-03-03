from datetime import date
import pandas as pd

def get_data():
    try:
        current_date = date.today().strftime("%Y_%m_%d")
        df = pd.read_csv(f"data/{current_date}.csv")
        return df
    except FileNotFoundError:
        print(f"No data file found for date {current_date}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error reading data: {e}")
        return pd.DataFrame()