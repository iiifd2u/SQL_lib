import sqlite3
from typing import List
import re
import numpy as np
import colorama
from colorama import Fore, Back, Style
import pandas as pd
from typing import List, Tuple
colorama.init(autoreset=True)


class Table_SQL():
    def __init__(self, name, name_db='titanic.db'):
        self.__name = name
        self.__name_db = name_db
        self.__con = sqlite3.connect(self.__name_db)
        self.__cur = self.__con.cursor()
        self.print_query = False
        self.print_info = False

    def get_name_table(self):
        return self.__name
    def __types_converter_sql(self, *types)->List[str]:

        new_types = []
        for el in types:
            if el == str:
                new_types.append('VARCHAR(255)')
            elif el == np.int64:
                new_types.append('INTEGER')
            elif el == np.float64:
                new_types.append('FLOAT')
            else:
                new_types.append(None)
        return new_types

    def __set_data(self, data:pd.DataFrame, max_value=1000)->List[Tuple]:
        return [tuple(data.iloc[i, :]) for i in range(min(len(data), max_value))]

    def __change_types_dataframe(self, data: pd.DataFrame) -> pd.DataFrame:
        change_list = []
        for idx in range(len(data.columns)):
            if type(data.iloc[1, idx]) == np.int64:
                change_list.append(data.columns[idx])
        data.loc[:, change_list] = data.loc[:, change_list].astype(float)
        return data

    def __typization(self, data: pd.DataFrame) -> dict:

        types = [type(el) for el in data.iloc[1, :]]
        input_dict = {x: y for x, y in zip(data.columns, types)}
        return input_dict

    def __print_query(self, query):
        if self.print_query:
            print(*query, sep='\n\t')
    def __print_info(self, info):
        if self.print_info:
            print(info)


    def create_columns(self, data):
        data = self.__change_types_dataframe(data)
        named_columns = self.__typization(data)
        try:
            if self.__cur is None:
                self.__con = sqlite3.connect(self.__name_db)
                self.__cur = self.__con.cursor()

            self.columns_names = named_columns.keys()
            self.columns_types = self.__types_converter_sql(*named_columns.values())
            columns_and_types = [' '.join(list(el)) for el in zip(self.columns_names, self.columns_types)]
            query = 'CREATE TABLE IF NOT EXISTS ' + self.__name + '(id INTEGER PRIMARY KEY AUTOINCREMENT, '\
                    + ', '.join(columns_and_types) + ')'
            query_print = re.split(r'(,)', query)
            query_print = [''.join([el1, el2]) for el1, el2 in zip(query_print[::2], query_print[1::2])]
            self.__print_query(query_print)

            self.__cur.execute(query)
            self.__con.commit()
            self.__print_info(Fore.LIGHTGREEN_EX+'Table has been created successful!')
        except sqlite3.Error as error:
            self.__print_info(Fore.LIGHTRED_EX +str(error))
    def insert_table(self, data: pd.DataFrame, max_value =1000):

        data = self.__set_data(data, max_value=max_value)
        try:
            if self.__cur is None:
                self.__con = sqlite3.connect(self.__name_db)
                self.__cur = self.__con.cursor()

            data = [tuple([i] + list(data[i])) for i in range(len(data))]
            v = '(' + ','.join(['?'] * (len(data[0]))) + ')'
            query = 'INSERT INTO ' + self.__name + ' VALUES' + v
            #print(query)
            #print('\t', *data, sep='\n\t')
            self.__cur.executemany(query, data)
            self.__con.commit()
            self.__print_info(Fore.LIGHTGREEN_EX+'Table has been edit successful!')

        except sqlite3.Error as error:
            self.__print_info(Fore.LIGHTRED_EX +str(error))
    def update_table(self, **kwargs:dict):
        try:
            if self.__cur is None:
                self.__con = sqlite3.connect(self.__name_db)
                self.__cur = self.__con.cursor()

            for key, value in kwargs.items():
                for item, el in value.items():
                    #print('UPDATE ' + self.__name + " SET " + item + " = '" + str(el) + "' WHERE id = " + key)
                    self.__cur.execute('UPDATE ' + self.__name + " SET " + item + " = '" + str(el) + "' WHERE id = " + key)
            self.__con.commit()
            self.__print_info(Fore.LIGHTGREEN_EX+'Table has been updated successful!')
        except sqlite3.Error as error:
            self.__print_info(Fore.LIGHTRED_EX +error)
    def convert_different_funcs(self, dif_func:str)->str:
        func_stack = re.split(r'[\+\-\*\/]', dif_func)
        #print(func_stack)
        func_stack = re.split(r'[\(\)]', ''.join(func_stack))
        return ' '.join(func_stack)

    def select_table(self,  FROM=['*'], AGR_FUNC='', AS = '', WHERE='', GROUP_BY='', ORDER_BY='', reversed=False, LIMIT=''):
        try:
            if self.__cur is None:
                self.__con = sqlite3.connect(self.__name_db)
                self.__cur = self.__con.cursor()

            if FROM!=['*']:
                FROM=['id']+FROM
            query = 'SELECT ' + ', '.join(FROM)

            query_agregate = ''
            if AGR_FUNC != '':
                query_agregate = ', ' + AGR_FUNC

            query_as = ''
            if AS != '':
                query_as = AS

            query_where = ''
            if WHERE!='':
                query_where = ' WHERE '+WHERE

            query_group =''
            if GROUP_BY!='':
                query_group =' GROUP BY '+GROUP_BY

            query_order=''
            if ORDER_BY != '':
                query_order = ' ORDER BY '+ORDER_BY +' DESC'*(reversed)

            query_limit=''
            if LIMIT!='':
                query_limit = ' LIMIT '+str(LIMIT)

            full_query = [query+query_agregate+' AS '*bool(query_as)+query_as, ' FROM '+ self.__name, query_where, query_group, query_order, query_limit]
            self.__print_query(full_query)

            # title:
            print('RESULT:')
            print(' '.join(FROM) if FROM!=['*'] else ' '.join(list(self.columns_names)),
                   re.sub(r', ', '', [query_agregate, query_as][bool(query_as)]))
            for row in self.__cur.execute(' '.join(full_query)):
                print(*list(row))

            self.__print_info(Fore.LIGHTGREEN_EX + 'SELECT was done successful!')
        except sqlite3.Error as error:
            self.__print_info(Fore.LIGHTRED_EX +str(error))

    def drop_table(self):
        try:
            if self.__cur is None:
                self.__con = sqlite3.connect(self.__name_db)
                self.__cur = self.__con.cursor()

            self.__cur.execute('DROP TABLE IF EXISTS ' + self.__name)
            self.__con.commit()
            self.__con.close()
            self.__print_info(Fore.LIGHTGREEN_EX+'Table has been deleted successful!')
        except sqlite3.Error as error:
            self.__print_info(Fore.LIGHTRED_EX +str(error))


