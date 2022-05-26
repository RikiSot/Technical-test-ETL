from pathlib import Path

from etl.dbconnection import DBConnection
from etl.functions import load_df_from_csv, pipeline


def mongodb():
    """
    This method should not be used, only works with mongo
    """
    mongodb = DBConnection(create_db=True, create_engine=True, dbtype='mongo')
    csv_path = Path('data/online retail.csv')
    df = load_df_from_csv(csv_path)
    # Drop table if exists
    mongodb.drop_table('online_retail')
    mongodb.create_table_from_df('online_retail', df)
    return mongodb.read_table_to_df('online_retail')


def mysqldb():
    """
    This method should not be used, only works with mysql. Gets the query with SQL syntax
    """
    mysqldb = DBConnection(create_db=True, create_engine=True, dbtype='mysql')
    csv_path = Path('data/online retail.csv')
    df = load_df_from_csv(csv_path)
    # Drop table if exists
    mysqldb.drop_table('online_retail')
    mysqldb.create_table_from_df('online_retail', df)
    sql_df = mysqldb.read_table_to_df('online_retail')
    # Query the dataframe with SQL syntax
    query = 'SELECT MONTH(InvoiceDate) as Month, SUM(UnitPrice) as "Monthly Revenue" FROM test.online_retail ' \
            'WHERE YEAR(InvoiceDate) = 2011 and Country = "United Kingdom" ' \
            'GROUP BY MONTH(InvoiceDate)' \
            'ORDER BY MONTH(InvoiceDate)'
    sql_filtered = mysqldb.execute_query_to_df(query)
    return sql_df, sql_filtered


if __name__ == '__main__':
    # MySQL case
    df_mysql, sql_filtered = mysqldb()
    filtered_mysql = DBConnection.get_monthly_sum_df(df_mysql, 'United Kingdom', 2011)
    print(filtered_mysql)

    # MongoDB case
    df_mongo = mongodb()
    filtered_mongo = DBConnection.get_monthly_sum_df(df_mongo, 'United Kingdom', 2011)
    print(filtered_mongo)

    # General case
    df_filtered = pipeline(dbtype='mongo', csv_path='data/online retail.csv', table_name='online_retail', country='United Kingdom', year=2011)
    print(df_filtered)
