from dotenv import load_dotenv, find_dotenv
import os
import pandas as pd

def load_env_secrets(dotenv_path, dbtype='mysql'):
    """Loads env variables from a .env file located in the project root
    directory.
    """
    load_dotenv(find_dotenv(dotenv_path))
    if dbtype == 'mysql':
        db_settings = {
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT'),
            'dbname': os.getenv('DB_NAME')
            
        }
    elif dbtype == 'mongo':
        db_settings = {
            'user': os.getenv('MONGO_DB_USER'),
            'password': os.getenv('MONGO_DB_PASSWORD'),
            'host': os.getenv('MONGO_DB_HOST'),
            'port': int(os.getenv('MONGO_DB_PORT')),
            'dbname': os.getenv('MONGO_DB_NAME')
        }
    return db_settings


def read_csv_data(filename):
    """
    Reads data from a CSV file into a dataframe.
    """
    try:
        df = pd.read_csv(filename, parse_dates=True, infer_datetime_format=True, low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv(filename, parse_dates=True, infer_datetime_format=True, encoding='unicode_escape', low_memory=False)
    except Exception as e:
        print(e)
        raise e
    return df

def load_df_from_csv(csv_path):
    df = read_csv_data(csv_path)
    df = adapt_df_dtypes(df)
    return df

def adapt_df_dtypes(df):
    """
    Converts dataframe dtypes to SQL compatible dtypes for online retail csv.
    """
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])

    #TODO: replace NaNs for nulls
    #df.fillna(None, inplace=True)
    return df