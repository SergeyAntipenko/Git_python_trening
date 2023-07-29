from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy import Column,String,BIGINT
import fake_useragent
from bs4 import BeautifulSoup
from aiohttp import ClientSession
import requests
import json
import asyncio
import time 
import pandas as pd

Base = declarative_base()

class Vacancies_tabl(Base):
    __tablename__= "vacancies"
    id =Column(BIGINT,primary_key=True)
    company_name= Column(String)
    position = Column(String)
    job_description = Column(String)
    key_skills = Column(String)
    industries = Column(String)

    def __repr__(self):
        return (f'{self.id},{self.company_name},{self.position},self{self.job_description},{self.key_skills},{self.industries}')
    

class Vacancies_grep():
    def get_vacancies(connect_string,url_html,api_url,text,per_page,search_field):
        global session
        engine_db= create_engine(connect_string)
        session = Session(engine_db)
        p_count =0
        url= url_html.format(text=text,search_field=search_field,per_page=per_page,page_num=p_count)
        ua = fake_useragent.UserAgent()
        resp = requests.get(
            url=url,
            headers={"user-agent":str(ua.safari)}
        )
        if resp.status_code != 200:
            return 0
        
        soup = BeautifulSoup(resp.content,'lxml')
        
        try:
            page_num = int(soup.find("div",class_="pager").find_all("span",recursive=False)[-1].find('a').find("span").text)
           
        except:
            page_num = 0     

        if page_num == 0 :
            Vacancie_id = http_parser(soup)
            if len(Vacancie_id) > 0:
                asyncio.run(api_vacancies(api_url,Vacancie_id) )  
        else:
           
            Vacancie_id=http_parser(soup)
            if len(Vacancie_id) > 0:

                asyncio.run(api_vacancies(api_url,Vacancie_id) ) 
                p_count+=1
            while page_num > p_count:
                time.sleep(5)

                url = url_html.format(text=text,search_field=search_field,per_page=per_page,page_num=p_count)
                resp= requests.get(
                        url=url,
                        headers={"user-agent":str(ua.safari)}
                    )
                if resp.status_code != 200:
                    return 0    
                
                soup = BeautifulSoup(resp.content , "lxml")
                Vacancie_id=http_parser(soup)
                if len(Vacancie_id) > 0:
                    asyncio.run(api_vacancies(api_url,Vacancie_id) )  
                p_count+=1 
    
    def insert_skill(connect_string):
        sql_script= ''' 
insert into skills (id_vacancia,skill_name)
select t.id, t.skill_name 
from (select v.id, unnest(string_to_array(v.key_skills, '; '))as skill_name from vacancies v where v.key_skills !='') t
left join  skills as s on s.id_vacancia = t.id and s.skill_name = t.skill_name
where 1=1 
and trim(t.skill_name) != ''
and s.id is null'''
        engine_db= create_engine(connect_string)
        session = Session(engine_db)   
        session.execute(sql_script)
        session.commit()
        session.close()     

    def top_skill(connect_string):   
        script =''' select s.skill_name, count(*) as ret from  skills s 
 where s.id_vacancia in  
 (select  v.id from telecom_companies tc 
  inner  join  vacancies v on  lower(v.company_name) = lower(replace(substring(tc."name",strpos(tc."name",'"'),length(tc."name")),'"','')) 
 union 
  select  t.id  from (select * ,unnest(string_to_array(v.industries, '; ')) as i  from  vacancies v ) t
where t.i like '9.399%'  or t.i like '9.40%' )
  group by s.skill_name
  order by 2 desc
  limit 10 '''
        engine_db= create_engine(connect_string)
        session = Session(engine_db)   
        result = session.execute(script).fetchall()
        fd = pd.DataFrame(result)
      
        print(fd)


def http_parser(soup_http):
    Vacancie_id=[]
    data_v = json.loads(soup_http.find('template', attrs={'id': 'HH-Lux-InitialState'}).text)

    for item in data_v['vacancySearchResult']['vacancies']:
        if session.query(Vacancies_tabl).filter(Vacancies_tabl.id == int(item['vacancyId'])).first() is None:
            Vacancie_id.append(int(item['vacancyId']))
        
    return Vacancie_id

async def api_vacancies(api_url,Vacancie_id):
    async with ClientSession(api_url) as Api_session:
        task=[]
        i =0 
        for id in Vacancie_id:
            tasks = asyncio.create_task(get_api(id,Api_session))
            task.append(tasks)
            if i%5 == 0:
                await(tasks)
                await asyncio.sleep(5)

        results = await asyncio.gather(*task)
    session.add_all(results)
    session.commit()


async def get_api(id,Api_session):
    industries = ''
    url = f'/vacancies/{id}'
    async with Api_session.get(url=url) as resp:
        json_vacancie = await resp.json()
    try:    
        id_e = json_vacancie['employer']['id']
    except Exception as e:
        print(e)
        industries = 'No'
    
    if industries != 'No':
        url_i= f'/employers/{id_e}'
        async with Api_session.get(url=url_i) as resp_e:
            json_employers = await resp_e.json()
            
        industries = ''.join(str(x['id']) +'; ' for x in  json_employers['industries']) #json_employers['industries'].get('id')
    

    job_description= str( BeautifulSoup(json_vacancie['description'], "lxml"))
    key_skills=''.join(str(x['name']) +'; ' for x in  json_vacancie['key_skills'])
    company_name= json_vacancie['employer']['name']
    position = json_vacancie['name']

    
    return Vacancies_tabl(id=id,company_name= company_name,position=position,job_description=job_description,key_skills =key_skills,industries=industries)

#vacancies_grep.top_skill('postgresql+psycopg2://proj:pas123@127.0.0.1:5432/project')
# vacancies_grep.get_vacancies('postgresql+psycopg2://proj:pas123@127.0.0.1:5432/project',
#                              'https://hh.ru/search/vacancy?no_magic=true&L_save_area=true&text={text}&search_field={search_field}&excluded_text=&area=1&salary=&currency_code=RUR&experience=doesNotMatter&order_by=relevance&search_period=0&items_on_page={per_page}&page={page_num}',
#                              'https://api.hh.ru/',
#                              'Python+middle',
#                              20,
#                              0,
#                              'name')