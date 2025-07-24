import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import pandahouse as ph
import io

import telegram

connection = {
    'host': 'https://clickhouse.internal.company.com',
    'user': 'analyst_user',
    'password': 'secure_password_123',
    'database': 'analytics_db'
}

def test_report(chat = None):
    chat_id = chat or -938659451

    my_token = 'secret_token'
    bot = telegram.Bot(token = my_token)
    
    query = '''
        select toDate(time) as date
             , countIf(action = 'view') as views
             , countIf(action = 'like') as likes
             , likes/views as CTR
             , uniqExact(user_id) as DAU
        from analytics_db.feed_actions
        where date between today() - 7 and today() - 1
        group by date
        order by date
            '''
    data = ph.read_clickhouse(query, connection = connection)
    
    sns.set(rc = {'figure.figsize':(21,9)}, style = 'whitegrid') # наводим красоту
    
    # карточка
    plt.subplot(2, 2, 1)
    plt.title('Ежедневный отчёт KPI', fontsize = 25, pad = 20)
    plt.figtext(0.12, 0.81, f"Данные за {str(max(data['date'].dt.date))}", ha = "left", fontsize = 18)
    plt.figtext(0.13, 0.75, f"Активных пользователей = {data['DAU'].loc[6]}", ha = "left", fontsize = 15)
    plt.figtext(0.13, 0.70, f"Просмотров = {data['views'].loc[6]}", ha = "left", fontsize = 15)
    plt.figtext(0.13, 0.65, f"Лайков = {data['likes'].loc[6]}", ha = "left", fontsize = 15)
    plt.figtext(0.13, 0.60, f"CTR = {round(data['CTR'].loc[6] * 100,2)} %", ha = "left", fontsize = 15)
    plt.axis('off')
    
    # DAU
    plt.subplot(2, 2, 2)
    sns.barplot(x=data['date'].dt.date, y=data['DAU'], color = 'b')
    plt.title('Активные пользователи (DAU)', fontsize = 25, pad = 20)
    plt.ylim(bottom = min(data['DAU']) - min(data['DAU']) * .05, top = max(data['DAU']) * 1.01)
    plt.xlabel(''), plt.ylabel('')
    
    # VIEWS / LIKES
    plt.subplot(2, 2, 3)
    sns.lineplot(x=data['date'].dt.date, y=data['views'], color = 'b', label = 'views')
    sns.lineplot(x=data['date'].dt.date, y=data['likes'], color = 'r', label = 'likes')
    plt.figtext(0.3, 0.03, 'Просмотры и лайки по дням', ha = "center", fontsize = 25)
    plt.ylim(bottom = 0)
    plt.xlabel(''), plt.ylabel('')
    
    # CTR
    plt.subplot(2, 2, 4)
    sns.barplot(x=data['date'].dt.date, y=data['CTR'], color = 'b')
    plt.figtext(0.72, 0.03, 'Конверсия из просмотра в лайк (CTR)', ha = "center", fontsize = 25)
    plt.ylim(bottom = min(data['CTR']) - min(data['CTR']) * .05, top = max(data['CTR']) * 1.05)
    plt.xlabel(''), plt.ylabel('')

    # вывод
    plot_object = io.BytesIO()
    plt.savefig(plot_object)

    plot_object.seek(0)

    plot_object.name = 'test_plot.png'
    plt.close()
    
    bot.sendPhoto(chat_id = chat_id, photo = plot_object)
    
    # Второй график
    
    query_2 = '''
    select event_date 
         , sum(messages_received) as messages_received
         , sum(messages_sent) as messages_sent
         , sum(users_received) as users_received
         , sum(users_sent) as users_sent
         , users_sent / users_received as SRR 
    from analytics.summarized_metrics
    where event_date >= today() - 7
    group by event_date
    '''
    # Прописываем все колонки для удобства копирования
    data_2 = ph.read_clickhouse(query_2, connection = connection)
    
    # карточка + Senter-to-Reciever Ratio (SRR) как своя метрика
    plt.subplot(2, 2, 1)
    plt.title('Ежедневный отчёт по мессенджеру', fontsize = 25, pad = 20)
    plt.figtext(0.12, 0.81, f"Данные за {str(max(data_2['event_date'].dt.date))}", ha = "left", fontsize = 18)
    plt.figtext(0.13, 0.75, f"{round((data_2['users_sent'].loc[6] / data['DAU'].loc[6] * 100),2) }% пользователей отправили сообщения", ha = "left", fontsize = 15)
    plt.figtext(0.13, 0.70, f"{round((data_2['users_received'].loc[6] / data['DAU'].loc[6] * 100),2) }% пользователей получили сообщения", ha = "left", fontsize = 15)
    plt.figtext(0.13, 0.63, f"{round(data_2['SRR'].loc[6] * 100,2)}% коэффицент охвата рассылки", ha = "left", fontsize = 15)
    plt.axis('off')
    
    # Сообщения
    
    plt.subplot(2, 2, 2)
    plt.title('Активность сообщений', fontsize = 25, pad = 20)
    sns.lineplot(x=data_2['event_date'].dt.date, y=data_2['messages_sent'], color = 'b', label = 'отправлено')
    sns.lineplot(x=data_2['event_date'].dt.date, y=data_2['messages_received'], color = 'r', label = 'получено')
    plt.xlabel(''), plt.ylabel('')
    
    # SRR
    
    plt.subplot(2, 2, 3)
    plt.figtext(0.3, 0.03, 'Коэффицент охвата рассылки (SRR)', ha = "center", fontsize = 25)
    sns.lineplot(x=data_2['event_date'].dt.date, y=data_2['SRR'], color = 'b')
    plt.ylim(bottom = min(data_2['SRR']) - min(data_2['SRR']) * .1, top = max(data_2['SRR']) * 1.1)
    plt.xlabel(''), plt.ylabel('')
    
    # Пользователи
    
    plt.subplot(2, 2, 4)
    plt.figtext(0.72, 0.03, 'Активность пользователей', ha = "center", fontsize = 25)
    sns.lineplot(x=data_2['event_date'].dt.date, y=data_2['users_sent'], color = 'b', label = 'отправители')
    sns.lineplot(x=data_2['event_date'].dt.date, y=data_2['users_received'], color = 'r', label = 'получатели')
    plt.xlabel(''), plt.ylabel('')
    
    # отправка

    plot_object = io.BytesIO()
    plt.savefig(plot_object)

    plot_object.seek(0)

    plot_object.name = 'test_plot.png'
    plt.close()
    
    bot.sendPhoto(chat_id = chat_id, photo = plot_object)

from datetime import datetime, timedelta
import requests

from airflow.decorators import dag, task

default_args = {
    'owner': 'v-sulikaev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 5, 3),
}

schedule_interval = '0 8 * * *'

@dag(default_args = default_args, schedule_interval = schedule_interval, catchup = False)
def svs_report():
    
    @task
    def make_report():
        test_report()
    
    make_report()
    
svs_report = svs_report()
