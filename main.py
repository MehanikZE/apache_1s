import requests
from airflow import DAG
from airflow.models import Base
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# This is a sample Python script.
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from collections import OrderedDict
import glob
import psycopg2
import pandas as pd
# from sqlalchemy import create_engine
# from sqlalchemy.orm import Mapped
# from sqlalchemy.orm import Mapped, mapped_column, Session
from sqlalchemy_orm.session import Session
from wget import bar_thermometer
import zipfile
import wget
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')
from sqlalchemy import create_engine
# from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session
# from sqlalchemy.orm import DeclarativeBase
# from sqlalchemy.orm import Mapped
# from sqlalchemy.orm import mapped_column
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'superuser',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

def zagruzka():
    url = 'https://ofdata.ru/open-data/download/egrul.json.zip'
    # url = 'http://www.futurecrew.com/skaven/song_files/mp3/razorback.mp3'
    wget.download(url, out='~/airflow', bar=bar_thermometer)

def vichislenie_okved():
    okvd_f = '~/airflow/egrul.json.zip'
    with zipfile.ZipFile(okvd_f, 'r') as zip_file:
        file_names = zip_file.namelist()
        for name in file_names:
            with zip_file.open(name) as files:
                okved_df = pd.read_json(files, encoding='utf-8')
                # print(okved_df)
                kp_p = okved_df['data']
                for count, i in enumerate(kp_p):
                    w = i.setdefault('СвОКВЭД')
                    # print(w)
                    try:
                        t = w['СвОКВЭДДоп']
                        sd = t[1]
                        okved = sd['КодОКВЭД']
                        # print(okved)
                        if okved[:2] == '61':
                            print(f'Наш код', okved)
                            okv_d = [okved]
                            s = okved_df.iloc[[count]]
                            s['okvd'] = okv_d
                            print(f'Сплюсованный датафрейм', s)
                            engine = create_engine(sqlite_hook)
                            s[['ogrn', 'inn', 'kpp', 'okvd', 'name', 'full_name']].to_sql('telecom_companys', engine, if_exists='append')
                            print('Наш код для БД- Запись в БД отправлена.')
                    except:
                        print('Код не найден')

    # print("Вычисление оквед пропущено.")

def pars():
    user_agent = {"User-agent": "Mozilla/5.0"}
    url_api = 'https://api.hh.ru/vacancies?text=python%20middle&per_page=15'
    url_api1 = 'https://api.hh.ru/vacancies?text=python%20middle&per_page=30&industry=9'

    class Base(DeclarativeBase):
        pass

    class Vacan(Base):
        __tablename__ = "vacancies_1"

        index: Mapped[str] = mapped_column(primary_key=True)
        company_name: Mapped[str]
        position: Mapped[str]
        job_description: Mapped[str]
        key_skills: Mapped[str]

        def __repr__(self) -> str:
            return f"User(id={self.index!r}, name={self.company_name!r}, fullname={self.position!r}, fullname={self.key_skills!r})"
    #
    engine = create_engine(sqlite_hook)
    Base.metadata.create_all(engine)
    print("Таблица создана")

    print(f'Парсинг через API START')

    result = requests.get(url_api1, headers=user_agent)

    print(result.status_code)
    print(result.text)
    j = result.json()
    print('dddd', j)
    print(type(j))
    vacans = result.json().get('items')
    print('eeeee', vacans)
    ttt = []
    ttt = []
    list_kn = []
    slovar = {}
    for i, vac in enumerate(vacans):
        print(i + 1)  # vac['name'], vac['url'], vac['alternate_url'])
        s = vac['url']
        print(s)
        res = requests.get(s, headers=user_agent)
        vacs = res.json()
        m = vacs['name']
        g = vacs['employer']['name']
        z = vacs['description']
        key_skills = vacs['key_skills']
        if key_skills:
            list = []
            for sk in key_skills:
                # list = []
                l = sk['name']
                list.append(l)
        else:
            print("xczcszcsdcs")
            list = "No skills in vacancy"
        engine = create_engine(sqlite_hook)
        with Session(engine) as session:
            names = Vacan(index=i + 1, company_name=g, position=m, job_description=z, key_skills=l)
            session.add_all([names])
            session.commit()

        print(f'Комания:', m, g, z)
    for ch in list_kn:
        if ch not in slovar:
            slovar[ch] = 0
        slovar[ch]+=1

    sorted_values = sorted(slovar.values())
    sorted_dict = OrderedDict()
    for i in sorted_values:
        for k in slovar.keys():
            if slovar[k] == i:
                sorted_dict[k] = slovar[k]
                break
                if len(sorted_dict) == 10:
                    break
    y = dict(reversed(sorted_dict.items()))
# y = reversed(sorted_dict)
# print(type(slovar))
# print(f'fff', slovar)
# print(f'sort', sorted_dict)
    print(f'Топ 10 скилов', y)

with DAG(
    dag_id='Itog_v1_v301',
    default_args=default_args,
    description='proekt_itog',
    start_date=datetime(2023, 7, 26),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='nach_first_task',
        python_callable=zagruzka,
    )
    task2 = PythonOperator(
        task_id='nach_two_task',
        python_callable=vichislenie_okved,
    )
    task3 = PythonOperator(
        task_id='nach_therd_task',
        python_callable=pars,
    )
    task1.set_downstream(task2)
    task1.set_downstream(task3)
