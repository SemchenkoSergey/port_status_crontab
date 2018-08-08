#!/usr/bin/env python3
# coding: utf-8

import sys
import warnings

warnings.filterwarnings("ignore")


def main():
    while True:
        print('-')
        print('1. Снятие показаний с портов DSLAM')
        print('2. Получение количества абонентских сессий из Онимы')
        print('3. Обработка отчетов, формирование таблиц из Онимы')
        print()
        print('4. Выход из программы')
        number = input('-\n> ')
        if number == '4':
            sys.exit()
        elif number == '1':
            from resources import Port_Status
            print()
            Port_Status.main()
            sys.exit()
        elif number == '2':
            from resources import Session_Count
            print()
            Session_Count.main()
            sys.exit()
        elif number == '3':
            from resources import Make_Table
            print()
            Make_Table.main()
            sys.exit()
        else:
            print('\n!!! Нужно ввести число от 1 до 4 !!!\n')    


if __name__ == '__main__':
    main()
