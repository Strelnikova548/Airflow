import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse
import requests
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

def ch_get_df(query='Select 1', host='https://clickhouse.lab.karpov.courses', user='student', password=''):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(io.StringIO(r.text), sep='\t')
    return result

my_token = ''

bot = telegram.Bot(token=my_token)

chat_id = -9

default_args = {
    'owner': 'e-strelnikova-9',
    'depends_on_past': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2022, 8, 8)
}


schedule_interval = '0 11 * * *'

q= """SELECT


                    t1.day, t1.month, t1.user_id, t1.country, t1.source,
                    t1.views, t1.likes, t1. CTR, 
                    t2.messages, t2.messages_per_user
                    FROM (SELECT toDate(toStartOfDay(time)) AS day,
                    toDate(toStartOfMonth(day)) AS month,
                    user_id,
                    country,
                    source,
                    countIf(post_id, action='view') AS views,
                    countIf(post_id, action='like') AS likes,
                    round(likes / views, 2) as CTR,
                    FROM simulator_20220720.feed_actions
                    WHERE toDate(time) between today()-15 and yesterday()
                    GROUP BY day, user_id, os, gender, country, source
                    ORDER BY day ) AS t1
                    LEFT JOIN
                    ( SELECT toDate(toStartOfDay(time)) AS day, toDate(toStartOfMonth(day)) AS month,
                    user_id,
                    country,
                    source,
                    count(user_id) AS messages,
                    round(count(user_id) / countDistinct(user_id), 2) AS messages_per_user
                    FROM simulator_20220720.message_actions
                    WHERE toDate(time) between today()-15 and yesterday()
                    GROUP BY day, user_id, country, source
                    ORDER BY day) AS t2
                    ON t1.user_id = t2.user_id AND t1.day = t2.day
                    format TSVWithNames
                    """ 






@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_strelnikova_Lesson7_Ex2():

    @task()
    def extract_data(query):
        df = ch_get_df(query=query)
        return df

    @task()
    def send_message(df):
        df_yesterday = df[df['day'] == max(df['day'])]
        df_yesterday_group = df_yesterday.groupby('day', as_index=False).agg({'user_id': 'count', 'views': 'sum', 'likes' : 'sum', 'CTR': 'mean',  'messages':'sum', 'messages_per_user': 'mean'})
        dau=df_yesterday_group.iloc[0]['user_id']
        views=df_yesterday_group.iloc[0]['views']
        likes=df_yesterday_group.iloc[0]['likes']
        CTR=df_yesterday_group.iloc[0]['CTR']
        messages=df_yesterday_group.iloc[0]['messages']
        messages_per_user=df_yesterday_group.iloc[0]['messages_per_user']
        
        users_ads = round(df_yesterday[df_yesterday['source'] == 'ads'].shape[0] / df_yesterday.shape[0] ,4)
        users_organic = round(df_yesterday[df_yesterday['source'] == 'organic'].shape[0] / df_yesterday.shape[0] ,4)
    
        
        text_for_alert = '_'*15 + '\n' \
        + 'Ключевые показатели приложения за вчерашний день: \n\n' \
        + f'DAU: {dau:,} \n' \
        + f'Просмотры: {views:,} \n' \
        + f'Лайки: {likes:,} \n' \
        + f'CTR: {round(CTR*100,2)}% \n' \
        + f'Сообщения (всего): {messages} \n' \
        + f'Сообщения (в среднем на пользователя): {round(messages_per_user,2)} \n' \
        + '-'*15 + '\n' \
        + f'Органические пользователи: {round(users_organic*100,2)}% \n' \
        + f'Рекламных пользователей: {round(users_ads*100,2)}% \n' \

        
        bot.sendMessage(chat_id=chat_id, text=text_for_alert)

    @task()
    def make_final(df):
        df['date'] = pd.to_datetime(df['day'])
        curr_week=df['date'].max().isocalendar()
        last_week=df['date'].max() - pd.Timedelta(7, unit='day')
        last_week=last_week.to_pydatetime().isocalendar()
        last_week_df=df.loc[(df['date'].dt.isocalendar().week == last_week[1]) & (df['date'] <= (df['date'].max() - pd.Timedelta(7, unit='day')))]
        curr_week_df=df.loc[df['date'].dt.isocalendar().week == curr_week[1]]
        df_week_dynamic=pd.concat([last_week_df, curr_week_df])
        df_wd_group=df_week_dynamic.groupby(pd.Grouper(key='date', freq='W')).agg({'user_id': 'count', 'views': 'sum', 'likes' : 'sum', 'CTR': 'mean','messages_per_user': 'mean'})
        final_data=pd.concat([df_wd_group, df_wd_group.diff()[1:]])
        final_data.set_index(pd.Index(['Previous_week', 'This_week', 'Difference']),'user_id', inplace=True)
        return final_data
    
    @task()
    def send_csv(final_data, df):
        file_object = io.StringIO()
        final_data.to_csv(file_object)
        file_object.name = 'Динамика.csv'
        file_object.seek(0)
        bot.sendDocument(chat_id=chat_id, document=file_object)
    
    @task()
    def send_graph(final_data):
        diff_g=round(final_data.iloc[-1:]/final_data.iloc[0],4) *100
        long_df = pd.melt(diff_g)
        ax=sns.barplot(y = long_df.variable, x = long_df.value, estimator=sum, ci=False, orient='h')
        ax.set_title('Динамика основных показателей по сравнению с прошлой неделей, %')
        plot_object = io.BytesIO()
        plt.savefig(plot_object, bbox_inches='tight')
        plot_object.seek(0)
        plot_object.name = 'Динамика основных показателей по сравнению с прошлой неделей, %.jpg'
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    get_df  = extract_data(q)
    bot_send_message = send_message(get_df)
    make_df = make_final(get_df)
    bot_send_csv = send_csv(make_df, get_df)
    bot_send_graph = send_graph(make_df)
    
    
    
    
dag_strelnikova_Lesson7_Ex2 = dag_strelnikova_Lesson7_Ex2()