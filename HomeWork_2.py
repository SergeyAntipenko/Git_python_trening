import pathlib
import requests
import fake_useragent
import asyncio
import json 
import time

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase ,sessionmaker
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import Session
from bs4 import BeautifulSoup
from aiohttp import ClientSession
pass
config= {"url":"""https://hh.ru/search/vacancy?no_magic=true&L_save_area=true&text={text}&search_field={search_field}&excluded_text=&area=1&salary=&currency_code=RUR&experience=doesNotMatter&order_by=relevance&search_period=0&items_on_page={per_page}&page={page_num}""",
        "url_api":"https://api.hh.ru/",   # страница api
        "text":"Python+middle",    # текст поиска
        "per_page":"50",          # кол-во на одной странице
        "page":"0",
        "search_field":"name",     # поиск по какому полю осуществлять
        "db_file":"db2.db"         # название файла БД
        }


class Base(DeclarativeBase):
    pass

class Vacancie(Base):
    __tablename__='vacancies'
    id: Mapped[int]=mapped_column(primary_key=True)
    company_name: Mapped[str]
    position: Mapped[str]
    job_description: Mapped[str]
    key_skills: Mapped[str]
    def __repr__(self):
        return (f'{self.id},{self.company_name},{self.position},self{self.job_description},{self.key_skills}')
    
# Получаем страницу 
def get_vacancies(text,search_field,per_page):
    p_count = 0
    url = config["url"].format(text=text,search_field=search_field,per_page=per_page,page_num=p_count)
    ua = fake_useragent.UserAgent()
    resp= requests.get(
        url=url,
        headers={"user-agent":str(ua.safari)}
    )
    
    if resp.status_code != 200:
        return 0
    
    soup = BeautifulSoup(resp.content , "lxml")
    # смотрим сколько станиц найдино с вакансиями  если возникнет ошибка либо значений всего одна станица либо нечего ненайдено
    try:
        page_num = int(soup.find("div",class_="pager").find_all("span",recursive=False)[-1].find('a').find("span").text)
    except:
        page_num = 0 

    # если мы находим одну станицу выполяем парсер полученной станицы 
    # если страниц больше выполним по очередной запрос получения остальных страниц
    if page_num == 0:
        Vacancie_id=http_parser(soup)
        asyncio.run(api(Vacancie_id))
    else:
        Vacancie_id=http_parser(soup)
        asyncio.run(api(Vacancie_id))
        p_count+=1

        while page_num > p_count:
            time.sleep(5)

            url = config["url"].format(text=text,search_field=search_field,per_page=per_page,page_num=p_count)
            resp= requests.get(
                    url=url,
                    headers={"user-agent":str(ua.safari)}
                )
            if resp.status_code != 200:
                return 0    
            
            soup = BeautifulSoup(resp.content , "lxml")
            Vacancie_id=http_parser(soup)
            asyncio.run(api(Vacancie_id))
            p_count+=1

#  парсим полученную страницу http
def http_parser(soup_http):
    Vacancie_id=[]
    data_v = json.loads(soup_http.find('template', attrs={'id': 'HH-Lux-InitialState'}).text)

    for item in data_v['vacancySearchResult']['vacancies']:
        Vacancie_id.append(int(item['vacancyId']))

    return Vacancie_id


async def api(list_id):
    async with ClientSession(config['url_api']) as Api_session:
        task=[]
        i=0
        for id in list_id:
            tasks = asyncio.create_task(get_api_vacancie(id,Api_session))
            task.append(tasks)
            i+=1
            # дожтдаемся завершение запросов и делаем паузу
            if i%10 ==0:
                await(tasks)
                await asyncio.sleep(5)

        results = await asyncio.gather(*task)
        # Обработка данных 
        for result in results:
           # Проверка существует запись или нет
            if session_DB.query(Vacancie).filter(Vacancie.id==result['id']).first() is None:
                job_description= str( BeautifulSoup(result['description'], "lxml"))
                key_skills=''.join(str(x['name']) +'; ' for x in  result['key_skills'])
                rr= result['employer']['name']
                position = result['name']
                id = result['id']
                print(result['id'],result['name'], result['employer']['name'],rr,key_skills)
                vacancies1= Vacancie(id= int(id),
                                    company_name=rr,
                                    position=position,
                                    job_description=job_description,
                                    key_skills=key_skills)
                session_DB.add(vacancies1)
        # проверяем есть ли новые данные которые надо коммитеть      

        if len(session_DB.new) > 0:
            session_DB.commit()




async def get_api_vacancie(id,ApiSession):
    url = f'/vacancies/{id}'
    async with ApiSession.get(url=url) as resp:
        json_vacancie = await resp.json()

    return json_vacancie




def main():
    global session_DB
    connect_db = create_engine(f"sqlite:///{pathlib.PurePath(__file__).parents[0].joinpath(config['db_file'])}", echo=False)
    Base.metadata.create_all(bind=connect_db)
    Sesion_DB =sessionmaker(bind=connect_db)
    session_DB=Sesion_DB()
    get_vacancies(config["text"],config["search_field"],config["per_page"])
    session_DB.close()

if __name__=='__main__':
    main()