import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse as ph
from datetime import date 
import io 

import sys
import os

connection = {
    'host': 'https://clickhouse.internal.company.com',
    'user': 'analyst_user',
    'password': 'secure_password_123',
    'database': 'analytics_db'
}

def check_anomaly(df, metric, a = 4, n = 5): 
    # алгоритм поиска аномалий (межквартильный размах)
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25) 
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75) 
    df['iqr'] = df['q75'] - df['q25'] 
    df['up'] = df['q75'] + a * df['iqr']
    df['low'] = df['q25'] - a * df['iqr']
    # сгладим
    df['up'] = df['up'].rolling(n, center = True, min_periods = 1).mean()
    df['low'] = df['low'].rolling(n, center = True, min_periods = 1).mean()

    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0

    return is_alert, df 

def run_alerts(chat = None):
    chat_id = chat or -969316925
    bot = telegram.Bot(token = '7848904672:AAGe-boDiGL_yAB4ss75Q-fAUWYnqlYeUmY')

    query = '''with ms as (
                    select toStartOfFifteenMinutes(time) as ts
                         , count() as messages
                    from analytics_db.message_actions
                    where time >= today() - 1 and time < toStartOfFifteenMinutes(now())
                    group by ts
                    order by ts desc
                    )

                    select toStartOfFifteenMinutes(fa.time) as ts
                         , toDate(fa.time) as date 
                         , formatDateTime(toStartOfFifteenMinutes(fa.time), '%R') as hm
                         , uniqExact(user_id) as users_feed
                         , countIf(user_id, action = 'view') as views
                         , countIf(user_id, action = 'like') as likes
                         , likes/views as CTR
                         , ms.messages
                    from analytics_db.feed_actions fa
                    join ms
                        on toStartOfFifteenMinutes(fa.time) = ms.ts
                    where fa.time >= today() - 1 and fa.time < toStartOfFifteenMinutes(now())
                    group by ts, date, hm, ms.messages
                    order by ts
                    '''
    data = ph.read_clickhouse(query, connection = connection)

    metrics_list = ['users_feed', 'views', 'likes', 'CTR', 'messages']
    for metric in metrics_list:
        df = data[['ts', 'date', 'hm', metric]].copy()
        is_alert, df = check_anomaly(df, metric)

        if is_alert == 1:
            msg = '''
Метрика {metric}:
Текущее значение {current_val:.2f}
Отклонение от предыдущего значения {last_val_diff:.2%}
**здесь должна прикрепляться ссылка на дашборд**'''.format(metric=metric,
                                                           current_val = df[metric].iloc[-1],
                                                           last_val_diff = abs(1 - (df[metric].iloc[-1] / df[metric].iloc[-2])))
            sns.set(rc = {'figure.figsize': (16, 10)})
            plt.tight_layout()

            ax = sns.lineplot(x = df['ts'], y = df[metric], label = 'metric')
            ax = sns.lineplot(x = df['ts'], y = df['up'], label = 'up')
            ax = sns.lineplot(x = df['ts'], y = df['low'], label = 'low')
 
            for ind, label in enumerate(ax.get_xticklabels()):
                if ind % 2 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)

            ax.set(xlabel = 'time')
            ax.set(ylabel = metric)

            ax.set_title(metric)
            ax.set(ylim = (0, None))

            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()

            bot.sendMessage(chat_id = chat_id, text = msg)
            bot.sendPhoto(chat_id = chat_id, photo = plot_object)

from datetime import datetime, timedelta
import requests

from airflow.decorators import dag, task

default_args = {
    'owner': 'v-sulikaev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 5, 25),
}

schedule_interval = '*/15 * * * *'

@dag(default_args = default_args, schedule_interval = schedule_interval, catchup = False)
def alert_svs():
    
    @task
    def make_report():
        run_alerts()
    
    make_report()
    
alert_svs = alert_svs()