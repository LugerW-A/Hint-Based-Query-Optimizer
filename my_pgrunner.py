import psycopg2
import json
from time import time

class PGRunner():
    def __init__(self, db_name, user, password, host, port):
        self.conn = psycopg2.connect(database=db_name,user=user,password=password,host=host,port=port)
        self.cur = self.conn.cursor()
        self.cur.execute("load 'pg_hint_plan';")
    
    def getCost(self, sql):
        # start = time()
        self.cur.execute("explain (COSTS, FORMAT JSON) "+sql)
        rows = self.cur.fetchall()
        plan_json = rows[0][0][0]
        # stop = time()
        # print("time: ", stop - start)
        # start = time()
        # self.cur.execute("explain (COSTS, FORMAT JSON, ANALYSE) "+sql)
        # rows = self.cur.fetchall()
        # plan_json = rows[0][0][0]
        # stop = time()
        # print("time: ", stop - start)
        # input()
        return plan_json['Plan']['Total Cost']

pg_runner = PGRunner('imdb', 'postgres', '', '127.0.0.1', 5432)