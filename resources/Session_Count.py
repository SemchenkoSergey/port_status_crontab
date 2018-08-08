# coding: utf-8

import re
import time
import MySQLdb
import datetime
from resources import Onyma
from resources import SQL
from resources import Settings
from concurrent.futures import ThreadPoolExecutor


def get_onyma_params():
    options = {'table_name': 'abon_onyma',
               'str1': 'account_name, bill, dmid, tmid'}    
    onyma_param = SQL.get_all_table_data(**options)
    result = {}
    for param in onyma_param:
        result[param[0]] = {'bill' : param[1], 'dmid' : param[2], 'tmid' : param[3]}
    return result    
        
        
def run(arguments):
    count_processed = 0
    count_insert = 0
    count_update = 0
    count_tv = 0
    count_tech_data = 0
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    onyma = Onyma.get_onyma()
    account_list = arguments[0]
    onyma_param_list = arguments[1]

    prev_day = datetime.date.today() - datetime.timedelta(days=1)
    for account in account_list:
        account_name = account[0]
        account_tv = account[1]
        account_hostname = account[2]
        account_board = account[3]
        account_port = account[4]
        
        if account_name in onyma_param_list:
            bill = onyma_param_list[account_name]['bill']
            dmid = onyma_param_list[account_name]['dmid']
            tmid = onyma_param_list[account_name]['tmid']
        else:
            onyma_param = Onyma.find_account_param(onyma, account_name)
            if onyma_param == -1:
                onyma = Onyma.get_onyma()
                continue
            elif onyma_param is False:
                count_processed += 1
                options = {'cursor': cursor,
                           'table_name': 'data_sessions',
                           'str1': 'account_name, date, count',
                           'str2': '"{}", "{}", {}'.format(account_name, prev_day.strftime('%Y-%m-%d'), 0)}
                SQL.insert_table(**options)
                continue
            else:            
                bill, dmid, tmid = onyma_param
                options = {'cursor': cursor,
                           'table_name': 'abon_onyma',
                           'str1': 'account_name, bill, dmid, tmid'.format(),
                           'str2': '"{}", "{}", "{}", "{}"'.format(account_name, bill, dmid, tmid)}
                SQL.insert_table(**options)
                count_insert += 1
        data = Onyma.count_sessions(onyma, bill,  dmid,  tmid,  prev_day)
        tv = Onyma.update_tv(onyma, bill, prev_day)
        if (data == -1) or (tv == -1):
            onyma = Onyma.get_onyma()
            continue
        if (tv is True) and (account_tv == 'no'):
            options = {'cursor': cursor,
                       'table_name': 'abon_dsl',
                       'str1': 'tv = "yes"',
                       'str2': 'account_name = "{}"'.format(account_name)}
            SQL.update_table(**options)            
        count = data['count']
        if count == 0:
            onyma_param = Onyma.find_account_param(onyma, account_name)
            if onyma_param == -1:
                onyma = Onyma.get_onyma()
                continue
            elif onyma_param is False:
                count_processed += 1
                options = {'cursor': cursor,
                           'table_name': 'data_sessions',
                           'str1': 'account_name, date, count',
                           'str2': '"{}", "{}", {}'.format(account_name, prev_day.strftime('%Y-%m-%d'), 0)}                
                SQL.insert_table(**options)                
                continue            
            else:
                cur_bill, cur_dmid, cur_tmid = onyma_param
                if cur_bill != bill or cur_tmid != tmid or cur_dmid != dmid:
                    options = {'cursor': cursor,
                               'table_name': 'abon_onyma',
                               'str1': 'bill = "{}", dmid = "{}", tmid = "{}"'.format(cur_bill, cur_dmid, cur_tmid),
                               'str2': 'account_name = "{}"'.format(account_name)}
                    SQL.update_table(**options)                    
                    count_update += 1
                data = Onyma.count_sessions(onyma, bill,  dmid,  tmid,  prev_day)               
                if data == -1:
                    onyma = Onyma.get_onyma()
                    continue
                count = data['count']
        if data['hostname'] is not None:
            if (data['hostname'] != account_hostname) or (data['board'] != account_board) or (data['port'] != account_port):
                options = {'cursor': cursor,
                           'table_name': 'abon_dsl',
                           'str1': 'hostname = "{}", board = {}, port = {}'.format(data['hostname'], data['board'], data['port']),
                           'str2': 'account_name = "{}"'.format(account_name)}
                SQL.update_table(**options)
                count_tech_data += 1
                #print(account_name, data['hostname'], data['board'], data['port'])
        count_processed += 1
        options = {'cursor': cursor,
                   'table_name': 'data_sessions',
                   'str1': 'account_name, date, count',
                   'str2': '"{}", "{}", {}'.format(account_name, prev_day.strftime('%Y-%m-%d'), count)}
        SQL.insert_table(**options)        
    connect.close()
    del onyma
    return (count_processed, count_insert, count_update, count_tv, count_tech_data)


def main():
    # Начало
    while True:
        print('-')
        print('1. Запуск утром следующего дня')
        print('2. Запуск сейчас')
        number = input('-\n> ')
        if number == '1' or number == '2':
            break
    if number == '1':
        run_date = datetime.datetime.now().date()
        print('Проверка сессий начнется завтра после 5 часов утра...\n')
    else:
        run_date = datetime.datetime.now().date() - datetime.timedelta(days=1)
        print('Запуск проверки сессий...\n')
    
    while True:
        current_date = datetime.datetime.now().date()
        if (current_date != run_date) and (datetime.datetime.now().hour >= 5):
            print('Начало работы: {}'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            count_processed = 0
            count_insert = 0
            count_update = 0
            count_tv = 0
            count_tech_data = 0
            SQL.create_data_sessions()
            SQL.create_abon_onyma()
            options = {'table_name': 'abon_dsl',
                       'str1': 'account_name, tv, hostname, board, port',
                       'str2': 'account_name IS NOT NULL'}
            account_list = SQL.get_table_data(**options)            
            if len(account_list) == 0:
                print('\n!!! Необходимо сформировать таблицу abon_dsl !!!\n')
                return    
            onyma_param_list = get_onyma_params()
            arguments = [(account_list[x::Settings.threads_count], onyma_param_list)  for x in range(0,  Settings.threads_count)]
            while True:
                try:
                    arguments.remove(((), onyma_param_list))
                except:
                    break
      
            with ThreadPoolExecutor(max_workers=Settings.threads_count) as executor:
                result = executor.map(run, arguments)
           
            for count in result:
                count_processed += count[0]
                count_insert += count[1]
                count_update += count[2]
                count_tv += count[3]
                count_tech_data += count[4]

            print('\nОбработано: {}'.format(count_processed))
            print('Добавлено: {}'.format(count_insert))
            print('Обновлено данных Онимы: {}'.format(count_update))
            print('Обнаружено ТВ: {}'.format(count_tv))
            print('Обновлено тех. данных: {}\n'.format(count_tech_data))
            
            options = {'table_name': 'data_sessions',
                       'str1': 'date < DATE_ADD(CURRENT_DATE(), INTERVAL -{} DAY)'.format(Settings.days)}
            SQL.delete_table(**options)
            print('Завершение работы: {}'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            run_date = current_date
        else:
            time.sleep(60*10)
            continue
