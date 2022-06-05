import psycopg2
import sys
import time
import os
from psycopg2 import Error

def proccessPrimary(Standby_ip, Arbitr_ip):
    id = 1
    while (True):
        ping_result = os.system("ping -c 1 " + Arbitr_ip + " > /dev/null")
        if ping_result == 0:
            print("[Состояние сети] -> Связь с арбитром есть")
            try:
                # Подключиться к существующей БД
                connection = psycopg2.connect(user="postgres", password="12345", host=Standby_ip, port="5432",
                                              database="postgres")
                # Создание курсора для выполнения операций с базой данных
                cursor = connection.cursor()
                # SQL-запрос для проверки живучести БД
                sql_query = 'select 1;'
                # Выполнение команды
                cursor.execute(sql_query)
                connection.commit()
                print(f"{id}) - БД хоста Standby доступна")
                cursor.close()
                connection.close()
                id += 1
            except (Exception, Error) as error:
                print(f"{id}) - БД хоста Standby НЕдоступна")
                id += 1
        else:
            print("[Состояние сети] -> Связи с арбитром НЕТ")
        time.sleep(5)


def proccessStandby(Primary_ip):
    id = 1
    flag_start_replica = False
    while (True):
        try:
            # Подключиться к существующей БД
            connection = psycopg2.connect(user="postgres", password="12345", host=Primary_ip, port="5432",
                                              database="postgres")
            # Создание курсора для выполнения операций с базой данных
            cursor = connection.cursor()
            # SQL-запрос для проверки живучести БД
            sql_query = 'select 1;'
            # Выполнение команды
            cursor.execute(sql_query)
            connection.commit()
            print(f"{id}) - БД хоста Primary доступна")
            if (flag_start_replica == False):
                os.system(f"su - postgres -c 'pg_basebackup -h {Primary_ip} -D /var/lib/postgresql/10/main -U user_replica -w --wal-method=stream --write-recovery-conf > /dev/null'")
                os.system("su - postgres -c '/usr/lib/postgresql/10/bin/pg_ctl -D /etc/postgresql/10/main start > /dev/null'")
                print("Репликация успешно выполнена")
                flag_start_replica = True
            cursor.close()
            connection.close()
            id += 1
        except (Exception, Error) as error:
            print(f"{id}) - БД хоста Primary НЕдоступна")
            id += 1
        time.sleep(5)

def proccessArbitr(Primary_ip, Standby_ip):
    id = 1
    flag_promote = False
    while (True):
        ping_result = os.system("ping -c 1 " + Primary_ip + " > /dev/null")
        if ping_result == 0:
            print("[Состояние сети] -> Связь с Primary есть")
        else:
            print("[Состояние сети] -> Связи с Primary НЕТ")
            try:
                # Подключиться к существующей БД
                connection = psycopg2.connect(user="postgres", password="12345", host=Standby_ip, port="5432",
                                              database="postgres")
                # Создание курсора для выполнения операций с базой данных
                cursor = connection.cursor()
                # SQL-запрос для проверки живучести БД
                sql_query = 'select 1;'
                # Выполнение команды
                cursor.execute(sql_query)
                connection.commit()
                print(f"{id}) - БД хоста Standby доступна")
                if (flag_promote == False):
                    os.system(f"ssh postgres@{Standby_ip} '/usr/lib/postgresql/10/bin/pg_ctl -D /etc/postgresql/10/main promote > /dev/null'")
                    flag_promote = True
                    print("[Standby->Primary] Успешное выполнение promote реплики")

                cursor.close()
                connection.close()
                id += 1
            except (Exception, Error) as error:
                print(f"{id}) - БД хоста Standby НЕдоступна")
                id += 1
        time.sleep(10)



def main():
    try:
        Primary_ip = sys.argv[1]
        Standby_ip = sys.argv[2]
        Arbitr_ip = sys.argv[3]
        Mode = sys.argv[4]
    except:
        print("[ОШИБКА] Введите <primary-ip> <standby-ip> <arbitr-ip> <Режим:primary,stanby,arbitr>")
        sys.exit()

    if (Mode == "Primary"):
        proccessPrimary(Standby_ip, Arbitr_ip)
    elif (Mode == "Standby"):
        proccessStandby(Primary_ip)
    elif (Mode == "Arbitr"):
        proccessArbitr(Primary_ip, Standby_ip)


if __name__ == "__main__":
    main()
