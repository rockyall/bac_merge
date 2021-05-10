import time
import os
import datetime
# import pyodbc
import mysql.connector
from mysql.connector import errorcode


class db_server:
    def __init__(self):
        self.connection = self
        self.cursor = self

    def get_sql_connection(self, server, db, trusted_connection, username="", password=""):
        try:

            if(trusted_connection):
                self.connection = pyodbc.connect(
                    'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes;')
                print("Connection made local :(")
            else:
                self.connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +
                                                 server + ';DATABASE=' + db + ';UID = ' + username + ';PWD=' + password + ';')
                print("Connection made with secure username and password :(")

            self.cursor = self.connection.cursor(buffered=True)

        except Exception as ex:
            print(ex)
            self.cursor.close()

    def get_mysql_connection(self, server, db, username="", password=""):
        try:
            self.connection = mysql.connector.connect(
                user=username, password=password, host=server, database=db)
            self.cursor = self.connection.cursor()
        except Exception as ex:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
            self.conneciton.close()

    def insert_rows(self, tablename, fields=[], data=[]):
        try:
            rows_fields = ','.join(fields)
            original_teble_name = self.get_original_table_name(tablename)

            for item in data:
                print(f"\n\nInserting items: {item}")
                print(f"Table name: {original_teble_name}")

                caractertArray = []
                for itemy in range(len(item)):
                    caractertArray.append("%s")

                print(
                    f"This is the orginal table name for this inserrtion: {original_teble_name}")
                print(rows_fields)
                print(item)

                query = "insert into {0} ({1}) values ({2});".format(
                    original_teble_name, rows_fields, ','.join(caractertArray))
                self.cursor.execute(query, item)
                self.connection.commit()
                print(f"Inserted good...")

        except Exception as ex:
            print(ex)

    def update_rows_id(self, identifier, table_name, arr_Headers=[], arr_Data=[]):
        try:
            # Query example: UPDATE table_name SET field1 = new-value1, field2 = new-value2 [WHERE Clause]

            # item = ["value1","value2","value3","value4","value5","value6",]
            # arr_Headers = ["header1", "header2", "header3"]

            headers = ','.join(arr_Headers)
            original_teble_name = self.get_original_table_name(table_name)

            for item in arr_Data:
                print(f"\nUpdating: {item}")
                caractertArray = []
                for itemy in range(len(item)):
                    caractertArray.append("%s")

                print(
                    f"This is the orginal table name for this inserrtion: {original_teble_name}")
                print(headers)
                print(item)

                header_value_concat = []
                for itemY in range(len(arr_Headers)):
                    if(type(arr_Data[0][itemY]) == str):
                        arr_Data[0][itemY] = f"'{arr_Data[0][itemY]}'"
                    row = f"{arr_Headers[itemY]}={arr_Data[0][itemY]}"
                    header_value_concat.append(row)

                query = "update {0} SET {1} where Id = {2};".format(
                    original_teble_name, ','.join(header_value_concat), identifier)
                self.cursor.execute(query)
                self.connection.commit()
                print(f"Inserted good...")
        except Exception as ex:
            pritn(ex)

    def get_rows_table(self, table):
        original_teble_name = self.get_original_table_name(table)
        query = f"""select * from {original_teble_name};"""
        rows = self.cursor.execute(query)
        self.connection.commit()
        for row in rows:
            print(row)

    def get_row_by_id(self, table, id):
        original_teble_name = self.get_original_table_name(table)
        query = f"""select * from {original_teble_name} where id='{id}'; """
        rows = self.cursor.execute(query)
        self.connection.commit()
        for row in rows:
            print(row)

    def get_profile_id_by_product(self, table, product):
        try:
            original_teble_name = self.get_original_table_name(table)
            query = f"""select * from {original_teble_name} where Product='{product}'; """
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            if(len(rows) == 0):
                return 0
            for row in rows:
                return row[0]
        except Exception as ex:
            print(ex)
            return 0

    def get_rows_by_First_Last_Name(self, table, first_name, last_name):
        query = f"""select * from {table} where FirstName='{first_name}' and LastName = '{last_name}'; """
        rows = self.cursor.execute(query)
        self.connection.commit()
        for row in rows:
            print(row)

    def get_data_query(self, str_query):
        try:
            query = f"""{str_query}"""
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            if(len(rows) == 0):
                return 0
            for row in rows:
                print(row)
                return row
        except Exception as ex:
            print("Error in get data quert function in db_server")
            print(ex)
            return 0

    # Helpers functions
    def map_tablename_with_lowercase(self):
        tables = []
        sql = f"""SELECT table_name FROM information_schema.tables WHERE table_schema='db_finance';"""
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        for row in rows:
            tables.append(row[0])
        return tables

    def get_original_table_name(self, table):
        tempList = self.map_tablename_with_lowercase()
        original_teble_name = ""
        for itemz in range(len(tempList)):
            if(table == tempList[itemz].lower()):
                original_teble_name = tempList[itemz]
                break
        return original_teble_name

    def close_connection(self):
        self.cursor.close()
