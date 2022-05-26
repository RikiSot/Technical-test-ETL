import pandas as pd
from etl.dbconnection import DBConnection


def read_csv_data(filename):
    """
    Reads data from a CSV file into a dataframe.
    """
    try:
        df = pd.read_csv(filename, parse_dates=True, infer_datetime_format=True, low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv(filename, parse_dates=True, infer_datetime_format=True, encoding='unicode_escape',
                         low_memory=False)
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

    # TODO: replace NaNs for nulls
    # df.fillna(None, inplace=True)
    return df


def pipeline(dbtype: str, csv_path: str, table_name: str, country: str, year: int):
    """
    Pipeline to insert data from csv to database, and then query the data
    :param dbtype: 'mongo' or 'mysql'
    :param csv_path: path to csv file
    :param table_name: name of table to create
    :param country: country to filter
    :param year: year to filter
    :return filtered_df: dataframe with filtered data
    """
    # Load data from csv
    df = load_df_from_csv(csv_path)
    # Connect to database
    db = DBConnection(create_db=True, create_engine=True, dbtype=dbtype)
    # Drop database if exists (for testing)
    db.drop_table(table_name)
    # Create table from dataframe
    db.create_table_from_df(table_name, df)
    # Read table to dataframe
    df_from_database = db.read_table_to_df(table_name)
    # Query the dataframe (independently of the database)
    filtered_df = DBConnection.get_monthly_sum_df(df_from_database, country, year)
    return filtered_df
