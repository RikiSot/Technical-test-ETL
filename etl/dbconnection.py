import mysql.connector
import pandas as pd
import pymongo
from sqlalchemy import create_engine
from dotenv import load_dotenv, find_dotenv
import os


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


class DBConnection:
    """
    Class to interact with the database
    """

    def __init__(self, create_db=False, create_engine=True, dbtype='mysql'):
        self.dbtype = dbtype
        self.set_db_parameters()
        self.conn = self.set_connection()
        if create_db:
            self.create_database()
        if dbtype == 'mysql':
            if create_engine:
                self.engine = self.create_engine()
        print(self.conn)

    def set_connection(self):
        """
        Sets the connection to the database with mysql connector. Returns the connection object
        """
        if self.dbtype == 'mysql':
            conn = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                port=self.port,
                database=self.db
            )
            return conn
        elif self.dbtype == 'sqlite':
            pass
        elif self.dbtype == 'mongo':
            conn = pymongo.MongoClient(self.host, self.port)
            return conn

    def set_db_parameters(self, secrets_path='secrets.env'):
        """
        Sets the DB parameters from the .env file
        """
        db_settings = load_env_secrets(secrets_path, self.dbtype)
        self.host = db_settings['host']
        self.user = db_settings['user']
        self.password = db_settings['password']
        self.port = db_settings['port']
        self.db = db_settings['dbname']

    def create_database(self, db_name=None):
        """
        Creates a database in the SQL database or mongodb.
        """
        if db_name is None:
            db_name = self.db
        if self.dbtype == 'mysql':
            with self.conn.cursor() as cursor:
                cursor.execute("CREATE DATABASE IF NOT EXISTS {}".format(db_name))
                self.conn.commit()
        if self.dbtype == 'mongo':
            self.mongodb = self.conn[db_name]

    def show_databases(self):
        with self.conn.cursor() as cursor:
            cursor.execute("SHOW DATABASES")
            for db in cursor:
                print(db)

    def create_engine(self):
        """
        Creates a SQLAlchemy engine to interact with the database. Needed to use pandas dataframe
        """
        engine = create_engine('mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db}'.format(
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            db=self.db
        )
        )
        return engine

    def execute_query(self, query):
        """
        Executes a SQL query and returns the data
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            data = cursor.fetchall()
        return data

    def execute_query_to_df(self, query):
        """
        Executes a SQL or mongodb query and returns the data as a dataframe
        """
        if self.dbtype == 'mysql':
            df = pd.read_sql(query, self.engine)
            return df
        elif self.dbtype == 'mongo':
            df = pd.DataFrame(list(self.mongodb.find(query)))
            return df
        else:
            raise NotImplementedError

    def drop_table(self, table_name):
        """
        Drops a table from the database
        """
        if self.dbtype == 'mysql':
            with self.conn.cursor() as cursor:
                cursor.execute("DROP TABLE IF EXISTS {}".format(table_name))
                self.conn.commit()
        if self.dbtype == 'mongo':
            self.mongodb.drop_collection(table_name)

    def create_table_from_df(self, table_name, df):
        """
        Creates a table in the SQL database from a dataframe. Also works for mongodb
        """
        if self.dbtype == 'mysql':
            df.to_sql(table_name, self.engine, if_exists='append')
        elif self.dbtype == 'mongo':
            self.conn[self.db][table_name].insert_many(df.to_dict('records'))
        else:
            raise NotImplementedError

    def read_table_to_df(self, table_name, no_id=True):
        """
        Reads a table from the database into a dataframe and returns it
        """
        if self.dbtype == 'mysql':
            df = pd.read_sql(table_name, self.engine)
            return df
        elif self.dbtype == 'mongo':
            cursor = self.mongodb[table_name].find()
            df = pd.DataFrame(list(cursor))
            # Delete the _id
            if no_id:
                del df['_id']
            return df
        else:
            raise NotImplementedError

    @staticmethod
    def get_monthly_sum_df(df, country: str, year: int):
        """
        Filter df so the query is independent of the db used.
        """
        filtered = df[(df['Country'] == country) & (df['InvoiceDate'].dt.year == year)].sort_values(
            by='InvoiceDate').copy()
        # Set the index to the InvoiceDate column
        filtered['InvoiceDate'] = pd.to_datetime(filtered['InvoiceDate'])
        filtered = filtered.resample('M', on='InvoiceDate').sum()['UnitPrice']
        # Change date colum to get the month and the year as a date string
        filtered.index = filtered.index.strftime('%Y-%m')
        return filtered
