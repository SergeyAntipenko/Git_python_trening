import configparser 
import pathlib
from sqlalchemy import MetaData
from sqlalchemy import Table, Column, Integer, String,BIGINT
from sqlalchemy import create_engine
import logging
logging.basicConfig()

class Db:
    def create_table(connect_stirng):

        logger = logging.getLogger('sqlalchemy.engine')
        logger.setLevel(logging.INFO)
        Config = configparser.ConfigParser()

        Config.read(pathlib.Path(pathlib.PurePath(__file__).parents[0],'config.ini'))
        connect_db = create_engine(connect_stirng, echo=False)
        metadata_obj = MetaData()

        TelecomCompanies_table = Table("telecom_companies", metadata_obj,
            Column('ogrn',BIGINT,primary_key=True),
            Column('inn',String),
            Column('kpp',String),
            Column('full_name',String),
            Column('name',String),
            Column('okved',String)
        )

        Vacancies_table = Table("vacancies", metadata_obj,
            Column('id',BIGINT,primary_key=True),
            Column('company_name',String),
            Column('position',String),
            Column('job_description',String),
            Column('key_skills',String),
            Column('industries',String)
        )

        Skills_table = Table("skills", metadata_obj,
            Column('id',Integer,primary_key=True),
            Column('id_vacancia',Integer),
            Column('skill_name',String)
        )        

        metadata_obj.create_all(connect_db)
        
