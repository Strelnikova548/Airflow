# инпорт библиотек

import pandas as pd
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse as ph
from pandahouse.http import execute
from pandahouse.core import to_clickhouse, read_clickhouse

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context




# соединение с CH
connection = {'host' : 'https://clickhouse.lab.karpov.courses',
              'database' : 'simulator_20220720',
              'user' : 'student',
              'password' : ''}


# Дефолтные параметры, которые прокидываются в таски


default_args = {
'owner': 'e.strelnikova',
'depends_on_past': False,
'retries': 2,
'retry_delay': timedelta(minutes=5),
'start_date': datetime(2022, 8, 5),
}


# Интервал запуска DAG

schedule_interval = '0 23 * * *'

# Описание DAG

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_by_strelnikova():
    
    
    @task
    def feed_cube():
        #  лайки и просмотры по пользователям + выведение категории по возрасту
        query_fda = '''
                    SELECT toDate(time) AS event_date,
                           user_id,
                           if(gender = 1, 'male', 'female') AS gender,
                           multiIf(age < 12, 'children', age >= 12 and age < 15, 'younger_adolescence', age >= 15 and age < 18, 'older_adolescence', age >= 18 and age < 45, 'adult', 
                           age >= 45 and age < 60, 'middle-aged', age >= 60 and age < 75, 'elderly', 
                           'old aged') AS age,
                           os,
                           sum(action = 'like') AS likes,
                           sum(action = 'view') AS views
                    FROM simulator_20220720.feed_actions
                    WHERE toDate(time) = today() - 1
                    GROUP BY event_date, user_id, gender, age, os       
                    '''
        df_feed = ph.read_clickhouse(query_fda, connection = connection)
        return df_feed

    @task
    def msg_cube():
        # расчет сообщений по пользователям
        query_msg = '''
                    SELECT *
                    FROM
                    (SELECT toDate(time) AS event_date,
                          user_id,
                          if(gender=1, 'male', 'female') AS gender,
                          if(gender = 1, 'male', 'female') AS gender,
                          multiIf(age < 12, 'children', age >= 12 and age < 15, 'younger_adolescence', age >= 15 and age < 18, 'older_adolescence', age >= 18 and age < 45, 'adult', 
                           age >= 45 and age < 60, 'middle-aged', age >= 60 and age < 75, 'elderly', 
                           'old aged') AS age,
                          os,
                          count(reciever_id) AS messages_sent,
                          uniqExact(reciever_id) AS users_sent
                    FROM simulator_20220720.message_actions
                    WHERE toDate(time) = today() - 1
                    GROUP BY event_date, user_id, gender, age, os) t1
                    FULL OUTER JOIN
                    (SELECT toDate(time) AS event_date,
                           reciever_id AS user_id,
                           if(gender = 1, 'male', 'female') AS gender,
                           multiIf(age < 12, 'children', age >= 12 and age < 15, 'younger_adolescence', age >= 15 and age < 18, 'older_adolescence', age >= 18 and age < 45, 'adult', 
                           age >= 45 and age < 60, 'middle-aged', age >= 60 and age < 75, 'elderly', 
                           'old aged') AS age,
                           os,
                           count(user_id) AS messages_recieved,
                           uniqExact(user_id) AS users_recieved
                    FROM simulator_20220720.message_actions
                    WHERE toDate(time) = today() - 1
                    GROUP BY event_date, user_id, gender, age, os) t2
                    USING(user_id, event_date, gender, age, os)
                    '''
        df_msg = ph.read_clickhouse(query_msg, connection = connection)
        return df_msg
    
    @task
    def merge_cubes(df_feed, df_msg):
        # объединение таблиц и получение сводной информации по срезу пользователей
        df_cube = df_feed.merge(df_msg, on=['event_date','user_id', 'gender', 'age', 'os'], how='outer').fillna(0)
        return df_cube
    
    @task
    def transform_by_gender(df_cube):
        df_cube_gender = df_cube[['event_date','gender','views','likes','messages_recieved', 'messages_sent','users_recieved','users_sent']].groupby(
            ['event_date','gender'], as_index=False).agg('sum').rename(columns={'gender':'merge_category'})
        df_cube_gender['merged_by'] = 'gender'
        return df_cube_gender
    
    @task
    def transform_by_age(df_cube):
        df_cube_age = df_cube[['event_date','age','views','likes','messages_recieved', 'messages_sent','users_recieved','users_sent']].groupby(
                    ['event_date','age'], as_index=False).agg('sum').rename(columns={'age':'merge_category'})
        df_cube_age['merged_by'] = 'age'
        return df_cube_age
    
    @task
    def transform_by_os(df_cube):
        df_cube_os = df_cube[['event_date','os','views','likes','messages_recieved', 'messages_sent','users_recieved','users_sent']].groupby(
                    ['event_date','os'], as_index=False).agg('sum').rename(columns={'os':'merge_category'})
        df_cube_os['merged_by'] = 'os'
        return df_cube_os
    
    @task
    def concatenated_cube(df_cube_gender, df_cube_age, df_cube_os):
        # получение итоговой таблицы
        df_final = pd.concat([df_cube_gender, df_cube_age, df_cube_os]).reset_index(drop=True)
        df_final = df_final.reindex(columns=['event_date','merged_by','merge_category','views','likes','messages_recieved','messages_sent','users_recieved','users_sent'])
        df_final = df_final.astype({'views':'int64',
                                   'likes':'int64',
                                   'messages_recieved':'int64',
                                    'messages_sent':'int64',
                                    'users_recieved':'int64',
                                    'users_sent':'int64'})
        return df_final
    
    
    @task
    def load_cube(df_final):
        # создание таблицы 
        load_connection = {'host' : 'https://clickhouse.lab.karpov.courses',
                          'database' : 'test',
                          'user' : 'student-rw',
                          'password' : ''}
        
        query_create = '''
        CREATE TABLE IF NOT EXISTS test.strelnikova_table
            (   event_date Date,
                merged_by String,
                merge_category String,
                views UInt64,
                likes UInt64,
                messages_recieved UInt64,
                messages_sent UInt64,
                users_recieved UInt64,
                users_sent UInt64
                ) ENGINE = Log()
        '''
        ph.execute(connection=load_connection, query = query_create)
        
        ph.to_clickhouse(df=df_final, table='strelnikova_table', index=False, connection = load_connection)
        
    # выполнение таксов по DAGу
    #  сборка датафрейма по ленте
    df_feed = feed_cube()
    #  сборка датафрейма по мессенджеру
    df_msg = msg_cube()
    #  объединение датафреймов
    df_cube = merge_cubes(df_feed, df_msg)
    #  трансформ куба по гендеру
    df_cube_gender = transform_by_gender(df_cube)
    #  трансфорт куба по возрасту
    df_cube_age = transform_by_age(df_cube)
    #  трансформ куба по операционной системе
    df_cube_os = transform_by_os(df_cube)
    #  склеивание таблиц
    df_final = concatenated_cube(df_cube_gender, df_cube_age, df_cube_os)
    #   загрузка
    load_cube(df_final)


dag_by_strelnikova = dag_by_strelnikova()
    