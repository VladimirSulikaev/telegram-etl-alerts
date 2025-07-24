from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from airflow.decorators import dag, task

# Проект основан на данных из корпоративного хранилища событий пользователей. 
# В целях конфиденциальности все подключения и имена сущностей приведены в обезличенном виде.

connection = {
    'host': 'https://clickhouse.internal.company.com',
    'user': 'analyst_user',
    'password': 'secure_password_123',
    'database': 'analytics_db'
}

connection_test = {
    'host': 'https://clickhouse.internal.company.com',
    'user': 'rw_user',
    'password': 'strong_pass_456',
    'database': 'test_db'
}

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'v-sulikaev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 22),
}

# Интервал запуска DAG
schedule_interval = '20 16 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def sulikaev_dag7():

    @task()
    def extract_df():
        query = """
        with output as (
        select user_id as user
             , toDate(time) as event_date
             , count() as messages_sent
             , uniq(receiver_id) as users_sent
        from analytics_db.message_actions
        where event_date = today() -1
        group by user, event_date),

        input as (
        select receiver_id as user
             , toDate(time) as event_date
             , count() as messages_received
             , uniq(user_id) as users_received
        from analytics_db.message_actions
        where event_date = today() -1
        group by user, event_date),

        dimension as (
        select toDate(time) as event_date
             , user_id as user
             , os
             , gender
             , age
             , countIf(action = 'view') as views
             , countIf(action = 'like') as likes
        from analytics_db.feed_actions
        where event_date = today() -1
        group by user_id, os, gender, age, event_date),

        combo as (
        select user, event_date, messages_sent, users_sent, messages_received, users_received
        from input
        full outer join output
            on input.user = output.user
            and input.event_date = output.event_date
        where event_date = today() -1
        group by user, event_date, messages_sent, users_sent, messages_received, users_received
        )

        select user, event_date, os, gender, age, views, likes, messages_received, messages_sent, users_received, users_sent
        from dimension
        full outer join combo
            on dimension.user = combo.user
            and dimension.event_date = combo.event_date
        where event_date = today() -1
        """
        
        df = ph.read_clickhouse(query, connection = connection)
        
        melted = df.melt(
        id_vars = ['user', 'event_date', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent'],
        value_vars = ['age', 'os', 'gender'],
        var_name = 'dimension',
        value_name = 'dimension_value')

        melted = melted[['event_date', 'dimension', 'dimension_value' ,'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        
        return melted
    
    # трансформ по OS
    
    @task()
    def transform_os(melted):
        df_os = melted[(melted['dimension'] == 'os')]\
                [['event_date', 'dimension_value' ,'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]\
                .groupby(['event_date', 'dimension_value'], as_index = False).sum()

        dimension_col = 'os'
        df_os.insert(1, 'dimension', dimension_col)
        
        return df_os
    
    # трансформ по полу
    
    @task()
    def transform_gender(melted):
        df_gender = melted[(melted['dimension'] == 'gender')]\
                [['event_date', 'dimension_value' ,'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]\
                .groupby(['event_date', 'dimension_value'], as_index = False).sum()

        dimension_col = 'gender'
        df_gender.insert(1, 'dimension', dimension_col)
        
        return df_gender
    
    # трансформ по возрасту
    
    @task()
    def transform_age(melted):
        df_age = melted[(melted['dimension'] == 'age')]\
                [['event_date', 'dimension_value' ,'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]\
                .groupby(['event_date', 'dimension_value'], as_index = False).sum()

        dimension_col = 'age'
        df_age.insert(1, 'dimension', dimension_col)
        
        return df_age
    
    # прилепим таблицы друг к другу и создадим пустую табличку, загрузим туда данные
    
    @task()
    def load(df_os, df_gender, df_age):
        df_concat = pd.concat([df_os, df_gender, df_age])
        
        query_test = '''CREATE TABLE IF NOT EXISTS analytics.summarized_metrics
                    ( event_date        Date
                    , dimension         String
                    , dimension_value   String
                    , views             Int64
                    , likes             Int64
                    , messages_received Int64
                    , messages_sent     Int64
                    , users_received    Int64
                    , users_sent        Int64
                    )
                    ENGINE = MergeTree()
                    ORDER BY event_date
                    '''
        ph.execute(query_test, connection = connection_test)
        ph.to_clickhouse(df = df_concat, table = 'summarized_metrics', index = False, connection = connection_test)
        
    melted = extract_df()
    df_os = transform_os(melted)
    df_gender = transform_gender(melted)
    df_age = transform_age(melted)
    load(df_os, df_gender, df_age)
    
sulikaev_dag7 = sulikaev_dag7()
