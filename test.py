import psycopg2
import sys
import time
import os
from psycopg2 import Error
import asyncpg
import asyncio

def RestartInterface(Primary_ip, Standby_ip):
    #запускаем скрипт для сброса интерфейса Primary
    os.system(f"ssh root@{Primary_ip} '/./home/primary/restart.sh'")
    #time.sleep(10)
    os.system(f"ssh postgres@{Standby_ip} 'rm -rf /var/lib/postgresql/10/*'")
    os.system(f"ssh postgres@{Standby_ip} 'pg_basebackup -h {Primary_ip} -D /var/lib/postgresql/10/main -U user_replica -w --wal-method=stream --write-recovery-conf > /dev/null'")
    os.system(f"ssh postgres@{Standby_ip} '/usr/lib/postgresql/10/bin/pg_ctl -D /etc/postgresql/10/main start > /dev/null'")
    
    
    
async def DoQueries(Primary_ip, Standby_ip):
    id = 1
    # Подключиться к существующей БД
    connection = await asyncpg.connect(user="postgres", password="12345", host=Primary_ip, port="5432",
    database="postgres")
    while id < 501:
        if id == 250:
            # На половине обрываем связь
            RestartInterface(Primary_ip, Standby_ip)
            time.sleep(2)
        ping_result = os.system("ping -c 1 " + Primary_ip + " > /dev/null")
        if ping_result == 0:
            print(f"[Состояние сети {id}] -> Связь с primary есть")
            try:
                # SQL-запрос
                sql_query = f"INSERT INTO postgres (name, surname) VALUES ({id}, {id});";
                # Выполнение асинхронного запроса
                async with connection.transaction():
                    await connection.execute(sql_query)
                print(f"{id}) - БД хоста primary доступна")
                id = id + 1
                time.sleep(0.1)
            except (Exception, Error) as error:
                print(f"{id}) - БД хоста primary НЕдоступна")
        else:
            print("[Состояние сети] -> Связи с primary НЕТ")
    await connection.close()
    
def Difference(Primary_ip, Standby_ip):
    connection = psycopg2.connect(user="postgres", password="12345", host=Primary_ip, port="5432",
    database="postgres")
    # Создание курсора для выполнения операций с базой данных
    cursor = connection.cursor()
    ping_result = os.system("ping -c 1 " + Primary_ip + " > /dev/null")
    if ping_result == 0:
        print("[Состояние сети] -> Связь с primary есть")
        try:
            # SQL-запрос
            sql_query = f"SELECT COUNT(*) FROM postgres;";
            # Выполнение команды
            cursor.execute(sql_query)
            connection.commit()
            RecordsPrimary = cursor.fetchone()[0]
            # Выводим количество записей по итогу
            print(f"Количество записей в Primary {RecordsPrimary}")
        except (Exception, Error) as error:
            print(f"{id}) - БД хоста primary НЕдоступна")
    cursor.close()
    connection.close()
    
    connection = psycopg2.connect(user="postgres", password="12345", host=Standby_ip, port="5432",
    database="postgres")
    # Создание курсора для выполнения операций с базой данных
    cursor = connection.cursor()
    ping_result = os.system("ping -c 1 " + Standby_ip + " > /dev/null")
    if ping_result == 0:
        print("[Состояние сети] -> Связь с standby есть")
        try:
            # SQL-запрос
            sql_query = f"SELECT COUNT(*) FROM postgres;";
            # Выполнение команды
            cursor.execute(sql_query)
            connection.commit()
            RecordsStandby = cursor.fetchone()[0]
            # Выводим количество записей по итогу
            print(f"Количество записей в standby {RecordsStandby}")
        except (Exception, Error) as error:
            print(f"{id}) - БД хоста standby НЕдоступна")
    cursor.close()
    connection.close()

def main():
    try:
        Primary_ip = sys.argv[1]
        Standby_ip = sys.argv[2]
    except:
        print("[ОШИБКА] Введите <primary-ip> <standby-ip>")
        sys.exit()
    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(DoQueries(Primary_ip, Standby_ip))
    loop.close()
    
    Difference(Primary_ip, Standby_ip)
    
    




if __name__ == "__main__":
    main()
