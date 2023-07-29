<html>
<h3  align="center">Домашняя работа № 3. Python для ETL (итоговый проект)</h3><br>
<img src="https://github.com/SergeyAntipenko/Git_python_trening/blob/main/AirFlow_Graph.jpg">
<li>скопировать dag_project.py и Каталог Project на сервер в корневую папку Airflow dags.
<li> Настроить  подключение к БД в конфигурационном файле (Project/config.ini) connect_strin= <br>
 табличную часть dag создаст сомостоятельно 
<br>
<hr>
<li><b><u>create_table</u></b> -  Создание таблиц<br>
<img src="https://github.com/SergeyAntipenko/Git_python_trening/blob/main/tabls.jpg" alt=" Создание таблиц">
<li> <b><u>dowload_egrul</u></b> 
> выполняется загрузка в каталог Project (Если в коталаге уже есть загруженный файл и размер его совпадает загрузка не осуществляется )
<li><b><u>import_egrul</u></b>   -  производит разбор архива и загружает данные в БД 
[Импортрованные данные в таблицу]("https://github.com/SergeyAntipenko/Git_python_trening/blob/main/telecom_companies.csv")
egrul_process.py  - строка кода 91  unzip_file(pathlib.Path(pathlib.PurePath(__file__).parents[0],name_file),codeOKEVD)   закоментирована если раскоментировать  загрузка в таблицу будут выполняться. Изза длительного процесса импорта в таблицу. 


</html>
