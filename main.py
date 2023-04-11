import socketserver
import json
import struct
import sys
import time
import os
import storage
import model
import train
import baoctl
import math
import reg_blocker

import psycopg2
from MCTS import MCTS
from set_arm import set_arm
import random

from constants import (PG_OPTIMIZER_INDEX, DEFAULT_MODEL_PATH,
                       OLD_MODEL_PATH, TMP_MODEL_PATH)

def add_buffer_info_to_plans(buffer_info, plans):
    for p in plans:
        p["Buffers"] = buffer_info
    return plans

class BaoModel:
    def __init__(self):
        self.__current_model = None

    def select_plan(self, messages):
        start = time.time()
        # the last message is the buffer state
        # *arms, buffers = messages

        # if we don't have a model, default to the PG optimizer
        if self.__current_model is None:
            return PG_OPTIMIZER_INDEX

        # if we do have a model, make predictions for each plan.
        # arms = add_buffer_info_to_plans(buffers, arms)
        res = self.__current_model.predict(messages)
        idx = res.argmin()
        stop = time.time()
        print("Selected index", idx,
              "after", f"{round((stop - start) * 1000)}ms",
              "Predicted reward / PG:", res[idx][0],
              "/", res[0][0])
        return idx

    def predict(self, messages):
        # the last message is the buffer state
        plan, buffers = messages

        # if we don't have a model, make a prediction of NaN
        if self.__current_model is None:
            return math.nan

        # if we do have a model, make predictions for each plan.
        plans = add_buffer_info_to_plans(buffers, [plan])
        res = self.__current_model.predict(plans)
        return res[0][0]
    
    def load_model(self, fp):
        try:
            new_model = model.BaoRegression(have_cache_data=True)
            new_model.load(fp)

            if reg_blocker.should_replace_model(
                    self.__current_model,
                    new_model):
                self.__current_model = new_model
                print("Accepted new model.")
            else:
                print("Rejecting load of new model due to regresison profile.")
                
        except Exception as e:
            print("Failed to load Bao model from", fp,
                  "Exception:", sys.exc_info()[0])
            raise e
            

# class JSONTCPHandler(socketserver.BaseRequestHandler):
#     def handle(self):
#         str_buf = ""
#         while True:
#             str_buf += self.request.recv(1024).decode("UTF-8")
#             if not str_buf:
#                 # no more data, connection is finished.
#                 return
#             null_loc = str_buf.find("\n")
#             if null_loc != -1:
#                 json_msg = str_buf[:null_loc].strip()
#                 str_buf = str_buf[null_loc + 1:]
#                 if json_msg:
#                     try:
#                         if self.handle_json(json.loads(json_msg)):
#                             break
#                     except json.decoder.JSONDecodeError:
#                         print("Error decoding JSON:", json_msg)
#                         break
index_table = {
    "company_id_movie_companies": "company_id", 
    "company_type_id_movie_companies": "company_type_id", 
    "info_type_id_movie_info_idx": "info_type_id", 
    "info_type_id_movie_info": "info_type_id", 
    "info_type_id_person_info": "info_type_id", 
    "keyword_id_movie_keyword": "keyword_id",
    "kind_id_aka_title": "kind_id",
    "kind_id_title": "kind_id",
    "linked_movie_id_movie_link": "linked_movie_id",
    "link_type_id_movie_link": "link_type_id",
    "movie_id_aka_title": "movie_id",
    "movie_id_cast_info": "movie_id",
    "movie_id_complete_cast": "movie_id",
    "movie_id_movie_companies": "movie_id",
    "movie_id_movie_info_idx": "movie_id",
    "movie_id_movie_keyword": "movie_id",
    "movie_id_movie_link": "movie_id",
    "movie_id_movie_info": "movie_id",
    "person_id_aka_name": "person_id",
    "person_id_cast_info": "person_id",
    "person_id_person_info": "person_id",
    "person_role_id_cast_info": "person_role_id",
    "role_id_cast_info": "role_id"
}
PG_CONNECTION_STR = "dbname=imdb user=postgres host=localhost"

JOIN_TYPES = ["Nested Loop", "Hash Join", "Merge Join"]
LEAF_TYPES = ["Seq Scan", "Index Scan", "Index Only Scan", "Bitmap Index Scan"]
ALL_TYPES = JOIN_TYPES + LEAF_TYPES
ARM_NUM = 5
mcts = MCTS()
set_arm = set_arm()
bao_model = BaoModel()

sys.stdout = open('./log_round200_max5000.txt', "w")

buffer = "{\"cast_info_pkey\": 1, \"movie_id_cast_info\": 6, \"person_id_cast_info\": 6, \"person_role_id_cast_info\": 1, \"role_id_cast_info\": 1, \"keyword_pkey\": 4, \"movie_keyword_pkey\": 1, \"keyword_id_movie_keyword\": 6, \"movie_id_movie_keyword\": 6, \"name_pkey\": 4, \"title_pkey\": 6, \"kind_id_title\": 1, \"cast_info\": 52, \"movie_keyword\": 51, \"name\": 49, \"title\": 50, \"keyword\": 237}"

def bao_plan_json(plan):
    res = ""
    if plan['Node Type'] in ALL_TYPES:
        res = "{\"Node Type\": \"" + plan['Node Type'] + "\""
    else:
        res = "{\"Node Type\": \"Other\""
    res += ", \"Node Type ID\": 42"
    if 'Relation Name' in plan:
        res += ", \"Relation Name\": \"" + plan['Relation Name'] + "\""
    if 'Index Name' in plan and 'Relation Name' not in plan:
        if plan['Index Name'] in index_table:
            res += ", \"Relation Name\": \"" + index_table[plan['Index Name']] + "\""
        else:
            res += ", \"Relation Name\": \"" + plan['Index Name'][0:-5] + "\""
    res += ", \"Total Cost\": " + str(plan['Total Cost'])
    res += ", \"Plan Rows\": " + str(plan['Plan Rows'])
    if 'Plans' in plan:
        res += ", \"Plans\": ["
        res += bao_plan_json(plan['Plans'][0])
        if len(plan['Plans']) == 2:
            res += ", " + bao_plan_json(plan['Plans'][1])
        res += "]}"
        return res
    else:
        res += "}"
        return res

def GetOnOFF(flag):
    if flag:
        return "on"
    else:
        return "off"

def run_query(sql, n, flag = False):
    start = time.time()
    leading = mcts.find_leading(n)
    conn = psycopg2.connect(PG_CONNECTION_STR)
    cur = conn.cursor()
    candidate_plan = []
    if flag:
        for arm in range(ARM_NUM):
            set_arm.set_arm_options(arm)
            cur.execute("SET enable_hashjoin TO " + GetOnOFF(set_arm.enable_hashjoin))
            cur.execute("SET enable_mergejoin TO " + GetOnOFF(set_arm.enable_mergejoin))
            cur.execute("SET enable_nestloop TO " + GetOnOFF(set_arm.enable_nestloop))
            cur.execute("SET enable_indexscan TO " + GetOnOFF(set_arm.enable_indexscan))
            cur.execute("SET enable_seqscan TO " + GetOnOFF(set_arm.enable_seqscan))
            cur.execute("SET enable_indexonlyscan TO " + GetOnOFF(set_arm.enable_indexonlyscan))
            cur.execute("EXPLAIN (FORMAT JSON)" + sql)
            plan_without_leading = cur.fetchall()
            plan_without_leading_json = plan_without_leading[0][0][0]
            bao_plan_without_leading_json = json.loads("{\"Plan\": " + bao_plan_json(plan_without_leading_json['Plan']) + "}")
            candidate_plan.append(bao_plan_without_leading_json)
            cur.execute("EXPLAIN (FORMAT JSON)" + leading + sql)
            plan_with_leading = cur.fetchall()
            plan_with_leading_json = plan_with_leading[0][0][0]
            bao_plan_with_leading_json = json.loads("{\"Plan\": " + bao_plan_json(plan_with_leading_json['Plan']) + "}")
            # print(plan_without_leading_json)
            # print(plan_with_leading_json)
            candidate_plan.append(bao_plan_with_leading_json)
        # candidate_plan.append(json.loads(buffer))
        result = bao_model.select_plan(candidate_plan)
        set_arm.set_arm_options(int(result/2))
        cur.execute("SET enable_hashjoin TO " + GetOnOFF(set_arm.enable_hashjoin))
        cur.execute("SET enable_mergejoin TO " + GetOnOFF(set_arm.enable_mergejoin))
        cur.execute("SET enable_nestloop TO " + GetOnOFF(set_arm.enable_nestloop))
        cur.execute("SET enable_indexscan TO " + GetOnOFF(set_arm.enable_indexscan))
        cur.execute("SET enable_seqscan TO " + GetOnOFF(set_arm.enable_seqscan))
        cur.execute("SET enable_indexonlyscan TO " + GetOnOFF(set_arm.enable_indexonlyscan))
        if result %2 == 0:
            try:
                cur.execute("EXPLAIN (FORMAT JSON, ANALYSE)" + sql)
            except:
                return 120.0
        else:
            try:
                cur.execute("EXPLAIN (FORMAT JSON, ANALYSE)" + leading + sql)
            except:
                return 120.0
    else:
        try:
            cur.execute("EXPLAIN (FORMAT JSON, ANALYSE)" + sql)
        except:
                return 120.0
    stop = time.time()
    chose_plan = cur.fetchall()
    plan = json.loads("{\"Plan\": " + bao_plan_json(chose_plan[0][0][0]['Plan']) + "}")
    # print(plan)
    # input()
    # plan = add_buffer_info_to_plans(json.loads(buffer), [plan])[0]
    reward = chose_plan[0][0][0]['Execution Time']
    storage.record_reward(plan, reward, n)
    # cur.execute("EXPLAIN (FORMAT JSON, ANALYSE)" + sql)
    # plan1 = cur.fetchall()
        # cur.execute("EXPLAIN (COSTS, FORMAT JSON)" + sql)
        # plan2 = cur.fetchall()
        # print(type(plan1))
        # print(plan1[0])
        # print(type(plan1[0]))
        # print(plan1[0][0])
        # print(type(plan1[0][0]))
        # print(plan_without_leading[0][0][0], flush = True)
        # print(plan1[0][0][0], flush = True)
        # print(plan2[0][0][0], flush = True)
        # # print(type(plan1[0][0][0]))
        # print(plan1, flush = True)
        # print("{'Plan': " + bao_plan1_json + "}")
        # # print(plan2, flush = True)
        # input()
    # try: 
    #     conn = psycopg2.connect(PG_CONNECTION_STR)
    #     cur = conn.cursor()
    #     cur.execute(f"SET enable_bao TO {bao_select or bao_reward}")
    #     cur.execute(f"SET enable_bao_selection TO {bao_select}")
    #     cur.execute(f"SET enable_bao_rewards TO {bao_reward}")
    #     cur.execute("SET bao_num_arms TO 5")
    #     # cur.execute("SET statement_timeout TO 500000")
    #     cur.execute(sql)
    #     cur.fetchall()
    #     conn.close()
    #     stop = time.time()
    # except:
    #     return 120.0
    total = stop - start
    if total > 120.0:
        return 120.0
    return total

# class BaoJSONHandler(JSONTCPHandler):
#     def setup(self):
#         self.__messages = []
    
#     def handle_json(self, data):
#         if "final" in data:
#             message_type = self.__messages[0]["type"]
#             self.__messages = self.__messages[1:]

#             if message_type == "query":
#                 result = self.server.bao_model.select_plan(self.__messages)
#                 self.request.sendall(struct.pack("I", result))
#                 self.request.close()
#             elif message_type == "predict":
#                 result = self.server.bao_model.predict(self.__messages)
#                 self.request.sendall(struct.pack("d", result))
#                 self.request.close()
#             elif message_type == "reward":
#                 plan, buffers, obs_reward = self.__messages
#                 plan = add_buffer_info_to_plans(buffers, [plan])[0]
#                 storage.record_reward(plan, obs_reward["reward"], obs_reward["pid"])
#             elif message_type == "load model":
#                 path = self.__messages[0]["path"]
#                 self.server.bao_model.load_model(path)
#             else:
#                 print("Unknown message type:", message_type)
            
#             return True

#         self.__messages.append(data)
#         return False
                

# def start_server(listen_on, port):
#     model = BaoModel()

#     if os.path.exists(DEFAULT_MODEL_PATH):
#         print("Loading existing model")
#         model.load_model(DEFAULT_MODEL_PATH)
    
#     socketserver.TCPServer.allow_reuse_address = True
#     with socketserver.TCPServer((listen_on, port), BaoJSONHandler) as server:
#         server.bao_model = model
#         server.serve_forever()


# if __name__ == "__main__":
    # from multiprocessing import Process
    # from config import read_config

    # config = read_config()
    # port = int(config["Port"])
    # listen_on = config["ListenOn"]

    # print(f"Listening on {listen_on} port {port}")
    
    # server = Process(target=start_server, args=[listen_on, port])
    
    # print("Spawning server process...")
    # server.start()


if os.path.exists(DEFAULT_MODEL_PATH):
    print("Loading existing model")
    bao_model.load_model(DEFAULT_MODEL_PATH)

with open("../../HyperQO/workload/JOB_static.json") as f:
    queries = json.load(f)
random.seed(117)
random.shuffle(queries)
total_time = 0
for id in range(20000):
    sql = queries[id][0]
    if id < 100:
        sql_time = run_query(sql, id)
    else:
        sql_time = run_query(sql, id, True)
    total_time += sql_time
    print(id, sql_time, total_time, flush=True)
    if (id + 1) % 200 == 0:
        # if id <= 4999 or  id == 9999 or id == 14999:
        # if id <= 4999 :
        start = time.time()
        os.system("python3 baoctl.py --retrain")
        os.system("sync")
        stop = time.time()
        total_time += stop - start
        bao_model.load_model(DEFAULT_MODEL_PATH)
