<html>
<h3  align="center">Домашняя работа № 3. Python для ETL (итоговый проект)</h3><br>
<img src="https://github.com/SergeyAntipenko/Git_python_trening/blob/main/AirFlow_Graph.jpg">
<li>скопировать dag_project.py и Каталог Project на сервер в корневую папку Airflow dags.
<li> Настроить  подключение к БД в конфигурационном файле (Project/config.ini) connect_strin= <br>
 табличную часть dag создаст сомостоятельно 
<br>
<hr>
<li><font color="blue"><u>create_table</u></font>   -  Создание таблиц<br>
<img src="https://github.com/SergeyAntipenko/Git_python_trening/blob/main/tabls.jpg" alt=" Создание таблиц">
<li> dowload_egrul - выполняется загрузка в каталог Project (Если в коталаге уже есть загруженный файл и размер его совпадает загрузка не осуществляется )
<li>import_egrul   -  производит разбор архива и загружает данные в БД <a href="https://github.com/SergeyAntipenko/Git_python_trening/blob/main/telecom_companies.csv" >Импортрованные данные в таблицу</a>
egrul_process.py  - строка кода 91  unzip_file(pathlib.Path(pathlib.PurePath(__file__).parents[0],name_file),codeOKEVD)   закоментирована если раскоментировать  загрузка в таблицу будут выполняться. Изза длительного процесса импорта в таблицу. 


</html>
