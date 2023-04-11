from psqlparse import parse_dict
from time import time
from copy import deepcopy
import psycopg2
import math
import random
import json
import sys
from my_pgrunner import pg_runner

class ProcessSql():
    def __init__(self, sql):
        self.sql = sql
        # print(sql)
        self.parse_sql = parse_dict(self.sql)[0]["SelectStmt"]
        self.table_list = list(set([parse["RangeVar"]["relname"] for parse in self.parse_sql["fromClause"]]))
        self.alias_table_dict = {'ci': 'cast_info', 'k': 'keyword', 'mk': 'movie_keyword', 'n': 'name', 't': 'title', 'at': 'aka_title', 'cn': 'company_name', 'ct': 'company_type', 'it1': 'info_type', 'mc': 'movie_companies', 'mi': 'movie_info', 'it': 'info_type', 'mi_idx': 'movie_info_idx', 'it2': 'info_type', 'an1': 'aka_name', 'n1': 'name', 'rt': 'role_type', 'chn': 'char_name', 'an': 'aka_name', 'cc': 'complete_cast', 'cct1': 'comp_cast_type', 'cct2': 'comp_cast_type', 'kt': 'kind_type', 'cn1': 'company_name', 'cn2': 'company_name', 'kt1': 'kind_type', 'kt2': 'kind_type', 'lt': 'link_type', 'mc1': 'movie_companies', 'mc2': 'movie_companies', 'mi_idx1': 'movie_info_idx', 'mi_idx2': 'movie_info_idx', 'ml': 'movie_link', 't1': 'title', 't2': 'title', 'pi': 'person_info', 'a1': 'aka_name', 'miidx': 'movie_info_idx', 'it3': 'info_type'}
        self.table_id_dict = {'cast_info': 0, 'keyword': 1, 'movie_keyword': 2, 'name': 3, 'title': 4, 'aka_title': 5, 'company_name': 6, 'company_type': 7, 'info_type': 8, 'movie_companies': 9, 'movie_info': 10, 'movie_info_idx': 11, 'aka_name': 12, 'role_type': 13, 'char_name': 14, 'complete_cast': 15, 'comp_cast_type': 16, 'kind_type': 17, 'link_type': 18, 'movie_link': 19, 'person_info': 20}
        # self.id_table_dict = {0: 'cast_info', 1: 'keyword', 2: 'movie_keyword', 3: 'name', 4: 'title', 5: 'aka_title', 6: 'company_name', 7: 'company_type', 8: 'info_type', 9: 'movie_companies', 10: 'movie_info', 11: 'movie_info_idx', 12: 'aka_name', 13: 'role_type', 14: 'char_name', 15: 'complete_cast', 16: 'comp_cast_type', 17: 'kind_type', 18: 'link_type', 19: 'movie_link', 20: 'person_info'}

    def getJoinlList(self):
        join_list = []
        for parse in self.parse_sql["whereClause"]["BoolExpr"]["args"]:
            if "A_Expr" in parse:
                if "ColumnRef" in parse["A_Expr"]["rexpr"]:
                    left_table_name = self.alias_table_dict[parse["A_Expr"]["lexpr"]["ColumnRef"]["fields"][0]["String"]["str"]]
                    right_table_name = self.alias_table_dict[parse["A_Expr"]["rexpr"]["ColumnRef"]["fields"][0]["String"]["str"]]
                    left_table_id = self.table_id_dict[left_table_name]
                    right_table_id = self.table_id_dict[right_table_name]
                    if left_table_id < right_table_id:
                        join_list.append((left_table_id, right_table_id))
                    else:
                        join_list.append((right_table_id, left_table_id))
                    # print(parse)
                    # input()
            # else:
            #     print(parse)
            #     print(self.sql)
        join_list = list(set(join_list))
        return join_list

class MctsNode():
    def __init__(self, state, parent):
        self.state = state
        self.is_terminal = state.isTerminal()
        self.is_fully_expanded = False
        self.parent = parent
        self.visit_cnt = 0
        self.reward = 0
        self.children = {}

class MctsState():
    def __init__(self, table_cnt, join_list, max_step):
        self.table_cnt = table_cnt
        self.join_list = join_list
        self.max_step = max_step
        self.order_list = []
        self.current_step = 0

    def getPossibleActions(self):
        possible_actions = set()
        for join in self.join_list:
            if self.current_step == 0:
                possible_actions.add(join[0])
                possible_actions.add(join[1])
            else:
                if join [0] in self.order_list and join[1] not in self.order_list:
                    possible_actions.add(join[1])
                if join [0] not in self.order_list and join[1] in self.order_list:
                    possible_actions.add(join[0])
        return list(possible_actions)
    
    def takeAction(self, table_id):
        new_state = deepcopy(self)
        new_state.order_list.append(table_id)
        # new_state.order_list[new_state.current_step] = table_id
        new_state.current_step = new_state.current_step + 1
        return new_state

    def isTerminal(self):
        if self.current_step == self.table_cnt or self.current_step == self.max_step:
            return True
        return False

class Mcts():
    def __init__(self, sql, initial_node, search_limit, exploration_constant = 0.25):
        self.sql = sql
        self.search_limit = search_limit
        self.exploration_constant = exploration_constant
        self.root_node = initial_node
        self.id_table_dict = {0: 'cast_info', 1: 'keyword', 2: 'movie_keyword', 3: 'name', 4: 'title', 5: 'aka_title', 6: 'company_name', 7: 'company_type', 8: 'info_type', 9: 'movie_companies', 10: 'movie_info', 11: 'movie_info_idx', 12: 'aka_name', 13: 'role_type', 14: 'char_name', 15: 'complete_cast', 16: 'comp_cast_type', 17: 'kind_type', 18: 'link_type', 19: 'movie_link', 20: 'person_info'}
    
    def search(self):
        for _ in range(self.search_limit):
            node = self.selectNode()
            reward = self.getReward(node)
            self.backPropogate(node, reward)

    def selectNode(self):
        node = self.root_node
        while not node.is_terminal:
            best_value = float("-inf")
            best_node = None
            if node.is_fully_expanded:
                # UCB select
                for child in node.children.values():
                    node_value = child.reward + self.exploration_constant * math.sqrt(math.log(node.visit_cnt) / child.visit_cnt)
                    if node_value > best_value:
                        best_value = node_value
                        best_node = child
                node = best_node
            else:
                node = self.expand(node)
        return node
    
    def expand(self, node):
        possible_actions = node.state.getPossibleActions()
        action = random.choice(possible_actions)
        new_node = MctsNode(node.state.takeAction(action), node)
        node.children[action] = new_node
        if len(node.children) == len(possible_actions):
            node.isFullyExpanded = True
        return new_node
    
    def getReward(self, node):
        leadings = '/*+leading('
        for order in node.state.order_list:
            leadings = leadings + self.id_table_dict[order] + ' '
        sql = leadings + ')*/' + self.sql
        return 1000 / pg_runner.getCost(sql)

    def backPropogate(self, node, reward):
        while node is not None:
            node.visit_cnt += 1
            node.reward = (node.reward + reward) / 2
            node = node.parent
    
    def getBestLeadings(self, number):
        leadings = []
        node = self.root_node
        for i in range(number):
            best_value = float('-inf')
            best_child = None
            for key,value in node.children.items():
                if value.reward > best_value:
                    best_value = value.reward
                    best_child = key
            leadings.append(self.id_table_dict[best_child])
            node = node.children[best_child]
        res = '/*+leading('
        for table in leadings:
            res = res + table + ' '
        res += ')*/'
        return res

if __name__ == "__main__":
    # 重定向输出
    sys.stdout = open("my_test.txt", "w")
    # 打开SQL
    with open("workload/JOB_static.json", "r") as f:
        queries = json.load(f)
    random.seed(117)
    random.shuffle(queries)
    for i in range(10):
        start = time()
        process_sql = ProcessSql(queries[9880 + i][0])
        initial_state = MctsState(len(process_sql.table_list), process_sql.getJoinlList(), 5)
        initial_node = MctsNode(initial_state, None)
        mcts = Mcts(process_sql.sql, initial_node, 10)
        mcts.search()
        print(mcts.getBestLeadings(2))
        stop = time()
        print("time: ", stop - start)
    # print(process_sql.table_list)
    # print(set(process_sql.from_table))
    # print(process_sql.getJoinlList())
    # for i in range(20000):
    #     process_sql = ProcessSql(queries[i][0])
    #     # print(process_sql.table_list)
    #     # print(set(process_sql.from_table))
    #     print(process_sql.getJoinlList())