import os
import pandas as pd 
import sqlite3


dir = os.path.dirname(__file__)   # Получаем имя директории  в которой был запущен проект
#настройки
filenmae = {"okved_2":"okved_2.json",  #имя файла для обработки 
            "sqlite":"C:\sqlite\hw1.db"   #БД Sqlite
            }
# Скрипт  для создания таблицы okved  
script_create_table_okved = """CREATE TABLE IF NOT EXISTS okved( code VARCHAR(10) ,   parent_code VARCHAR(10),   section VARCHAR(5),   name VARCHAR(512),   comment VARCHAR(2500) );"""


def main ():
    global sqlite_connection
    # Подключение к BD
    try:
        sqlite_connection = sqlite3.connect(filenmae["sqlite"]) 
    except:
        print("При подключении к БД возникла проблема! ВЫполнение программы завершено!")
        exit()
    # Обработка файла
    okved_2(filenmae["okved_2"])
    print("Запись данных в таблицу выполнена!")

#Обработка файла
def okved_2(fn):
    #Читаем файл DataFrame pandas
    df = pd.read_json(dir + "\\" + filenmae["okved_2"])
    # Создаем таблицу в БД 
    try:
        sqlite_connection.execute(script_create_table_okved)
    except:
        print("При создание таблицы в БД Возникла ошибка! ВЫполнение программы завершено!")
        exit()

    #Выполняем запись в таблицу из DataFrame
    try:
        df.to_sql('okved',sqlite_connection,if_exists='replace',index=False)
    except:
        print("При записи данных в таблицу Возникла ошибка! ВЫполнение программы завершено!")
        exit()

if __name__=='__main__':

    main()

