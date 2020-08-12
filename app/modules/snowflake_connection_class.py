from snowflake.connector import connect  # Connect to Snowflake DB
import sys
import pandas as pd


class sf:

    def __init__(self, user, password, account):
        try:
            self.connection = connect(user=user, password=password, account=account)
            self.cursor = self.connection.cursor()
            print("Snowflake connection established!")
        except:
            print("Connection Failed!")

    def create_db(self, db):
        try:
            self.cursor.execute(F"CREATE DATABASE IF NOT EXISTS {db}")
            print(F"Database Created, {db}\n")
        except:
            print("Failure to Create Database.")
            self.cursor.close()
            sys.exit(1)

    def view_sys_info(self):
        query = "SHOW TERSE DATABASES";
        df = pd.read_sql_query(query, self.connection)
        lst = list(df["name"].values)
        return '\n'.join(lst)

    def view_tables(self, db):
        self.cursor.execute(f"USE DATABASE {db}")
        query = "select TABLE_NAME, TABLE_OWNER, ROW_COUNT, LAST_ALTERED from information_schema.tables where table_schema = 'PUBLIC' and TABLE_CATALOG = " + "\'" + f"{db}".strip(' \t\n\r') + "\';"
        df = pd.read_sql_query(query, self.connection)
        df['LAST_ALTERED'] = pd.to_datetime(df['LAST_ALTERED'], format='%Y%m%d').dt.strftime('%m/%d/%Y')
        return df

    def view_top(self, table):
        query = F"SELECT * FROM {table} LIMIT 1000;"
        df = pd.read_sql_query(query, self.connection)
        return df

    def query(self, query):
        df = pd.read_sql_query(query, self.connection)
        return df

    def fetchall(self):
        return self.cursor.fetchall()

    def close(self):
        self.cursor.close()

