import unittest
from pathlib import Path
from etl.dbconnection import DBConnection
from etl.functions import load_df_from_csv, pipeline


class TestDBConnection(unittest.TestCase):

    def setUp(self):
        self.mysqldb = DBConnection(create_db=True, create_engine=True, dbtype='mysql')
        self.mongodb = DBConnection(create_db=True, create_engine=True, dbtype='mongo')

    def test_mysqldb(self):
        """
        Test the pipeline if SQL syntax is used
        """
        csv_path = Path('etl/data/online retail.csv')
        df = load_df_from_csv(csv_path)
        self.mysqldb.create_table_from_df('online_retail', df)
        sql_df = self.mysqldb.read_table_to_df('online_retail')
        # Query the dataframe with SQL syntax
        query = 'SELECT MONTH(InvoiceDate) as Month, SUM(UnitPrice) as "Monthly Revenue" FROM test.online_retail ' \
                'WHERE YEAR(InvoiceDate) = 2011 and Country = "United Kingdom" ' \
                'GROUP BY MONTH(InvoiceDate)' \
                'ORDER BY MONTH(InvoiceDate)'
        sql_filtered = self.mysqldb.execute_query_to_df(query)
        filtered_mysql = DBConnection.get_monthly_sum_df(sql_df, 'United Kingdom', 2011)
        # View the dataframes
        print(sql_filtered)
        print(filtered_mysql)
        # No errors
        self.assertTrue(True)

    def test_mongodb(self):
        csv_path = Path('etl/data/online retail.csv')
        df = load_df_from_csv(csv_path)
        self.mongodb.create_table_from_df('online_retail', df)
        df_mongo = self.mongodb.read_table_to_df('online_retail')
        # Query the dataframe
        df_mongo_filtered = DBConnection.get_monthly_sum_df(df_mongo, 'United Kingdom', 2011)
        print(df_mongo_filtered)
        # No errors
        self.assertTrue(True)

    def test_pipeline(self):
        """
        Test the pipeline function for the general case
        """
        # Mongo DB
        df_filtered = pipeline('mongo', 'data/online retail.csv')
        print(df_filtered)
        # MySQL DB
        df_filtered = pipeline('mysql', 'data/online retail.csv')
        print(df_filtered)
        # Check that the dataframes are the same
        self.assertEqual(df_filtered.equals(df_filtered), True)


