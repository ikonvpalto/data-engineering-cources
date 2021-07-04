Настройка:
1. Установить Docker https://docs.docker.com/engine/install/
2. Выполнить docker-compose build из директории с проектом (сборка потребует некоторого времени)
3. Для правильного доступа к службам вне докера, добавить список хостов в хост файл системы (см. список хостов).
   Также эти службы доступны по адресу 127.0.0.1:<service_port>
4. Для доступа изнутри сервиса (контейнера) к другому сервису используйте соответственное название хоста
   из списка extra_hosts раздела x-hosts в файле docker-compose.yml (а не его ip адрес),
   например, airflow-webserver

Управление сервисами (контейнерами)*:
docker-compose up -d <service_name> - создание и запуск контейнера
docker-compose start <service_name> - запуск ранее созданого контейнера
docker-compose stop <service_name> - остановка контейнера
docker-compose down <service_name> - остановка и удаление контейнера

* Все команды нужно запускать из директории проекта

Для работы со всеми сервисами сразу (рекомендуемый вариант),
нужно выполнять вышеуказанные команды без указания сервиса:
docker-compose up -d (старт всех сервисов)
docker-compose stop (остановка всех сервисов)

Доступ внутрь контейнера (на подобии ssh):
docker-compose exec <service_name> bash (или sh, если bash не установлен)

Выполнить команду внутри контейнера:
docker-compose exec <service_name> <command>

Более детальную информации по работе с образами и контейнерами, ищите по запросу docker-compose.

Список сервисов**:

Python
------
Доступ через консоль:
docker-compose exec python bash

Директория с исходниками:
app/ (монтируется внутрь контейнера)

Hadoop
------
NameNode  http://127.0.0.1:5070/
DataNode  http://127.0.0.1:5075/
webhdfs   http://127.0.0.1:5070/
resourcemanager http://127.0.0.1:8088/
nodemanager     http://127.0.0.1:8042/

Spark
-----
Master: spark-master:7077
master web http://127.0.0.1:8080/
worker web http://127.0.0.1:8081/
shell http://127.0.0.1:4041/

открыть spark shell:
docker-compose exec spark-master spark-shell

Apache Airflow
--------------
Airflow Web http://127.0.0.1:8000/
Flower Web  http://127.0.0.1:5555/
Worker http://airflow-worker:8793

Dugs размещать в airflow/dags (монтируется внутрь контейнера)
Plugins в airflow/plugins (монтируется внутрь контейнера)

Greenplum
---------
Host:     greenplum
Port:     5433
User:     gpadmin
Password: secret

Postgresql
----------
Host:     postgres
Port:     5432
User:     postgres
Password: secret

** Актуальный список сервисов и открытых портов можно посмотреть в docker-compose.yml раздел services.

Список хостов:
127.0.0.1 python
127.0.0.1 postgres
127.0.0.1 greenplum
127.0.0.1 redis
127.0.0.1 airflow-webserver
127.0.0.1 airflow-scheduler
127.0.0.1 airflow-worker
127.0.0.1 airflow-flower
127.0.0.1 hadoop-namenode
127.0.0.1 hadoop-datanode
127.0.0.1 hadoop-resourcemanager
127.0.0.1 hadoop-nodemanager
127.0.0.1 spark-master
127.0.0.1 spark-worker

Для запуска PosgreSQL необходимо в папку ./docker/docker/postgres поместить папки с omdb и pagila