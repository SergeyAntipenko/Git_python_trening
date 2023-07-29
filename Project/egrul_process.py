
import urllib.request
from urllib.parse import urljoin
import logging
import pathlib
import os , sys 
import json 
from sqlalchemy import Table, Column, Integer, String,BIGINT
from  sqlalchemy import create_engine
from sqlalchemy import  insert
from sqlalchemy.orm import declarative_base,Session
from contextlib import closing
from zipfile import ZipFile



Base = declarative_base()

class TelecomCompanies_table(Base):
    __tablename__= "telecom_companies"
    ogrn =Column(BIGINT,primary_key=True)
    inn= Column(String)
    kpp = Column(String)
    full_name = Column(String)
    name = Column(String)
    okved = Column(String)
    def __repr__(self):
        return (f'{self.ogrn},{self.inn},{self.kpp},self{self.full_name},{self.name},{self.okved}')



class Process():
    def dowload_file(url,name_file):
        urlopen = urllib.request.urlopen
        Request = urllib.request.Request
        logging.basicConfig()
        logger = logging.getLogger('urllib3')
        logger.setLevel(logging.INFO)
        logger.info("star dowlood file")
        file_dir = pathlib.Path(pathlib.PurePath(__file__).parents[0],name_file)
        try:
            req = Request(urljoin(url,name_file))
            u = urlopen(req)
            meta = u.info()
        except Exception as e :
            logger.error(e)
    
        if os.path.isfile(file_dir):
            if (os.path.getsize(file_dir) !=  int(meta["Content-Length"])):
                logger.info('there is a file with the same name in the folder the size of this file = ' + str(os.path.getsize(file_dir)) +' the downloaded file has the size = ' + meta["Content-Length"] + ' deleting the old file,  the file in the folder will be deleted and downloaded again')
            else:
                logger.info('there is a file with the same name in the folder the size of this file = ' + str(os.path.getsize(file_dir)) +' the downloaded file has the size = ' + meta["Content-Length"] + ' no download required')
                return
            
        f = open(file_dir,'wb')
        file_size = int(meta["Content-Length"] )
        file_size_dl = 0
        block_sz = 8192
        
        i = 1 
        while True:
            buffer = u.read(block_sz)
            if not buffer and file_size_dl < file_size:
                logger.warning("There is a problem, restart the download.")
                req.headers["Range"] = "bytes=%s-%s" %(file_size_dl, file_size)
                u = urlopen(req)
            elif not buffer:
                break

            file_size_dl += len(buffer)
            f.write(buffer)
            
            if  file_size_dl /  8192     == 100000 * i  :    
                sys.stdout.write("Dowload: %3.2f%%, %d byte" % (file_size_dl * 100. / file_size, file_size_dl))
                sys.stdout.flush()
                i+=1

        f.close()
        logger.info("File download completed")

    def insert_file(connect_sting="",name_file="",codeOKEVD=""):
        global session
        logging.basicConfig()
        logger = logging.getLogger('sqlalchemy.engine')
        logger.setLevel(logging.WARNING)
        logger.info("star parser file")
        engine_db= create_engine(connect_sting)
        session = Session(engine_db,engine_db)
        
        # расскоментировать что бы выполнить  чтение файла  за коментированно так как процесс занимает длительное время 
        # unzip_file(pathlib.Path(pathlib.PurePath(__file__).parents[0],name_file),codeOKEVD)
    


def unzip_file(file_path,codeOKEVD):

    with closing(ZipFile(file_path)) as archive: 
        logging.info(" Count file = " + str(len(archive.infolist())) )
        i = 0
        
        for fil in archive.namelist():

            file_parser(json.loads(archive.read(fil)),codeOKEVD)


def file_parser(data_json,codeOKEVD ):
    list_obj =[]
    for i in data_json:
        if session.query(TelecomCompanies_table).filter(TelecomCompanies_table.ogrn == int(i['ogrn'])).first() is None:
            try:
                code = i["data"].get("СвОКВЭД")["СвОКВЭДОсн"].get("КодОКВЭД")
            except (KeyError,TypeError):
                code = ""


            if code[0:2] == codeOKEVD:
                
                tct=TelecomCompanies_table(ogrn =i['ogrn'],inn=i['inn'],kpp=i['kpp'],full_name=i['full_name'],name=i['name'],okved=code) # Заполнения LIST  -
                list_obj.append(tct)
    
    if len(list_obj) > 0:
        session.add_all(list_obj)
        session.commit()
