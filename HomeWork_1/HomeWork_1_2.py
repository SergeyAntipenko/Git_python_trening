import json
from zipfile import ZipFile
import sqlite3
from  datetime import  datetime
from contextlib import closing
import pathlib


# Настройка    sqlite   - путь к БД sqllite    egrul  - путь к файлу архива  работа с zip   0 - чтения файла из архива 1 - распаковка файла потом удаление 
config = {"egrul":"D:\Обучение\Python_29_05_2023\egrul.json.zip",   #Путь к акхиву
          "sqlite":"C:\sqlite\hw1.db",                              #путь к БД sqllite
          "codeOKEVD":"61",                                         # код по которому идет отбор  данных
          "TpZip":0                                                 # работа с zip   0 - чтения файла из архива 1 - распаковка файла потом удаление 
        
          }






# скрипт создания таблицы telecom_companies
script_create_table_telecom_companies = """CREATE TABLE IF NOT EXISTS telecom_companies( ogrn VARCHAR(15) ,   inn VARCHAR(10),   kpp VARCHAR(10),   full_name VARCHAR(255),   okved VARCHAR(8) );"""

# Скрипт записи  данных в таблицу   telecom_companies
script_insert_telecom_companies ="""INSERT INTO  telecom_companies (ogrn,inn,kpp,full_name,okved ) SELECT T.* FROM (SELECT ? AS ogrn, ? AS inn ,?  AS kpp ,? AS full_name ,? AS okved ) as T 
                                                left join telecom_companies tc
                                              on tc.ogrn = T.ogrn    WHERE tc.ogrn is null;"""


# счетчик записей в таблицу
count_insert = 1
# счетчик найденых данных для записей 
count_search = 1 
#  счетчик файлов с ошибками
count_error = 1

# разбор json
def parser_json(data):
    global count_insert,count_search
    # переменная код ОКВЭД
    code =""
    #  LIST  -  для сбора найдинных данных для записи в табоицу в одном файле
    list_ins =[]

    for i in data:
        # получение кода ОКВЭД  , обработка ошибок KeyError - если не найден тег  TypeError - если не правильный тип данных
        try:
            code = i["data"].get("СвОКВЭД")["СвОКВЭДОсн"].get("КодОКВЭД")
        except (KeyError,TypeError):
            code = ""

        # Проверка кода    КодОКВЭД     
        if code[0:2] == config["codeOKEVD"]:
          
            list_ins.append((i['ogrn'],i['inn'],i['kpp'],i['full_name'],code)) # Заполнения LIST  
              
    # проверка list_ins  по окончанию чтения файла если он не пустой выполняем запись в таблицу
    if len(list_ins) > 0:
        count_search += len(list_ins)

        try:
            res = cursor.executemany(script_insert_telecom_companies, list_ins)
        except:
            return 1  #проблема при обработки
        finally:
            count_insert +=res.rowcount
            sqlite_connection.commit()
    return 0 #файл обработан ок


# Создание таблицы 
def create_table():
    try:
        sqlite_connection.execute(script_create_table_telecom_companies)
    except:
        print("При создание таблицы в БД Возникла ошибка! ВЫполнение программы завершено! Обратитесь к разработчику")
        exit()
    


def main():
    global sqlite_connection ,cursor,count_error
    # Подключение к БД
    try:
        sqlite_connection = sqlite3.connect(config["sqlite"]) 
    except:
        print("При подключении к БД возникла проблема! ВЫполнение программы завершено! Обратитесь  к разработчику")
        exit()
    # Создание таблицы
    create_table()
    
    count_file = 0   # счетчик прохода файлов 

    cursor = sqlite_connection.cursor()

    with closing(ZipFile(config["egrul"])) as archive:
       
        count_all_file = len(archive.infolist())  # Получаем количество файлов
        # Проверка какой тип работы с архивом
        if config["TpZip" ] == 0: #чтение из архива 
            # Перебор файлов в архиве            
            for file in archive.namelist():
              
                data = json.loads(archive.read(file)) # Читаем файл   
                # Передаем в функцию на разбор json и запись в таблицу                
                result = parser_json(data)
                # При обработки json  произошла ошибка                
                if result ==1:
                    count_error +=1
                    print (datetime.today(), " Ошибка обработки файла данные не записаны", archive.read(file))
                         
                count_file+=1 # Информирования пользователя каждые 500 обарботыных файлов 
                if count_file % 500  == 0:
                    print(datetime.today(), ' Всего файлов: ' , count_all_file ,'Обработано:', count_file , ' Найден: ' , count_search , " Записано в БД: ", count_insert, "Ошибок :" , count_error)

                elif count_file ==count_all_file:
                    print(datetime.today(), ' Всего файлов: ' , count_all_file ,'Обработано:', count_file , ' Найден: ' , count_search , " Записано в БД: ", count_insert, "Ошибок :" , count_error)
        
        # тип работы с архивом через распаковку    
        elif config["TpZip" ]== 1:
            # читаем список файлов              
            for file in archive.namelist():
            
                archive.extract(file,pathlib.PurePath(__file__).parents[0]) # Извлекаем файл в директорию с программой    
                # Читаем файл
                with open(pathlib.Path(pathlib.PurePath(__file__).parents[0],file), "r", encoding="utf8") as read_file:
                    data = json.load(read_file)
                # Передаем в функцию на разбор json и запись в таблицу                         
                result = parser_json(data)
                
                # При обработки json  произошла ошибка                     
                if result ==1:
                    count_error +=1
                    print (datetime.today(), " Ошибка обработки файла данные не записаны", archive.read(file))

                count_file+=1 # Информирования пользователя каждые 500 обарботыных файлов 
                if count_file % 500  == 0:
                    print(datetime.today(), ' Всего файлов: ' , count_all_file ,'Обработано:', count_file , ' Найден: ' , count_search , " Записано в БД: ", count_insert, "Ошибок :" , count_error)

                elif count_file ==count_all_file:
                    print(datetime.today(), ' Всего файлов: ' , count_all_file ,'Обработано:', count_file , ' Найден: ' , count_search , " Записано в БД: ", count_insert, "Ошибок :" , count_error)
                # Удаление файла                
                try:
                    pathlib.Path(pathlib.PurePath(__file__).parents[0],file).unlink()
                except:
                    pass
               
        
    cursor.close()
    
if __name__=='__main__':
    print ("старт ",datetime.today())
    main()
    print ("Финишь ",datetime.today())
