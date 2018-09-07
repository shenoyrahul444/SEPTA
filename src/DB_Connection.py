import sqlite3
from sqlite3 import Error
"""
A class for making database connections and providing the reuse of the same instance
"""
class DB_Connection:

    # A class variable the will be created just once and passed around for every time any object requires the connections for Database operations
    # I have created a singleton implementation - Could be done in a more effective way using inner functions
    conn = None
    def __init__(self,db_file):
        try:
            if DB_Connection.conn == None:
                DB_Connection.conn = sqlite3.connect(db_file)
            self.conn = DB_Connection.conn
        except Error as e:
            print(e)
    def getConnection(self):
        return self.conn

    def create_table(self,query):
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            self.conn.commit()
        except Error as e:
            print(e)

    def execute(self,query):
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except Error as e:
            print(e)
            return None
