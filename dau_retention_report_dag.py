import io

import pandahouse as ph
import pandas as pd
import seaborn as sns
import telegram as tg

from matplotlib import pyplot as plt
from airflow.decorators import dag, task
from datetime import timedelta, datetime, date

default_args = {
    'owner': 'a.harchenko-16',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 3, 2),
}

schedule_interval = '0 9 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def app_report_sender():
    @task(retries=3, retry_delay=timedelta(minutes=10))
    def dau_extractor(connection):
        query_dau = """
        WITH feed_users AS
            (SELECT DISTINCT user_id, toDate(time) AS date
            FROM simulator_20250120.feed_actions
            WHERE toDate(time) BETWEEN yesterday() - 29 AND yesterday()),

        message_users AS
            (SELECT DISTINCT user_id, toDate(time) AS date
            FROM simulator_20250120.message_actions
            WHERE toDate(time) BETWEEN yesterday() - 29 AND yesterday()),

        feed_dau_table AS
            (SELECT date,
                    COUNT(user_id) AS feed_dau
            FROM feed_users
            GROUP BY date),

        message_dau_table AS
            (SELECT date,
                    COUNT(user_id) AS message_dau
            FROM message_users
            GROUP BY date)

        SELECT dau_table.date AS date,
                dau,
                feed_dau,
                message_dau
        FROM
            (SELECT date,
                    COUNT(user_id) AS dau
            FROM
                (SELECT GREATEST(feed_users.user_id, message_users.user_id) AS user_id,
                        GREATEST(feed_users.date, message_users.date) AS date
                FROM feed_users 
                FULL OUTER JOIN message_users
                ON feed_users.user_id = message_users.user_id 
                AND feed_users.date = message_users.date) AS all_users
            GROUP BY date) AS dau_table
        FULL OUTER JOIN feed_dau_table AS fdt ON dau_table.date = fdt.date
        FULL OUTER JOIN message_dau_table AS mdt ON dau_table.date = mdt.date"""
        dau_df = ph.read_clickhouse(query=query_dau, connection=connection)
        return dau_df
    
    @task(retries=3, retry_delay=timedelta(minutes=10))
    def rr_extractor(connection):
        query_rr = """
        WITH
        all_dates_table AS -- to make sure that no dates are skipped
            (SELECT *
            FROM
                (SELECT yesterday() - 29 + INTERVAL number DAY AS first_date
                FROM numbers(30)) AS first_date_table 
                CROSS JOIN
                (SELECT yesterday() - 29 + INTERVAL number DAY AS date
                FROM numbers(30)) AS date_table
            WHERE first_date <= date),

        -- feed retention calculation
        feed_first_date_table AS
            (SELECT user_id,
                    MIN(toDate(time)) AS first_date
            FROM simulator_20250120.feed_actions
            GROUP BY user_id
            HAVING MIN(toDate(time)) BETWEEN yesterday() - 29 AND yesterday()),

        feed_users_by_date AS
            (SELECT first_date,
                    toDate(time) AS date,
                    COUNT(DISTINCT user_id) AS users
            FROM feed_first_date_table
            JOIN simulator_20250120.feed_actions USING(user_id)
            WHERE toDate(time) < today()
            GROUP BY first_date,
                     toDate(time)),

        feed_days_numbers AS   
            (SELECT adt.first_date AS first_date,
                    users,
                    adt.date AS date,
                    ROW_NUMBER() OVER (PARTITION BY adt.first_date ORDER BY adt.date) AS day,
                    MAX(users) OVER (PARTITION BY adt.first_date) AS users_first_day
            FROM all_dates_table AS adt
            LEFT JOIN feed_users_by_date AS fubd
            ON fubd.date = adt.date
            AND fubd.first_date = adt.first_date),

        feed_retention_by_days AS
            (SELECT day,
                    users * 100 / users_first_day AS retention_rate
            FROM feed_days_numbers),

        -- message retention calculation
        message_first_date_table AS
            (SELECT user_id,
                    MIN(toDate(time)) AS first_date
            FROM simulator_20250120.message_actions
            GROUP BY user_id
            HAVING MIN(toDate(time)) BETWEEN yesterday() - 29 AND yesterday()),

        message_users_by_date AS
            (SELECT first_date,
                    toDate(time) AS date,
                    COUNT(DISTINCT user_id) AS users
            FROM message_first_date_table
            JOIN simulator_20250120.message_actions USING(user_id)
            WHERE toDate(time) < today()
            GROUP BY first_date,
                     toDate(time)),

        message_days_numbers AS   
            (SELECT adt.first_date AS first_date,
                    users,
                    adt.date AS date,
                    ROW_NUMBER() OVER (PARTITION BY adt.first_date ORDER BY adt.date) AS day,
                    MAX(users) OVER (PARTITION BY adt.first_date) AS users_first_day
            FROM all_dates_table AS adt
            LEFT JOIN message_users_by_date AS mubd
            ON mubd.date = adt.date
            AND mubd.first_date = adt.first_date),

        message_retention_by_days AS
            (SELECT day,
                    users * 100 / users_first_day AS retention_rate
            FROM message_days_numbers)

        SELECT day,
                ROUND(AVG(frr.retention_rate), 2) AS feed_avg_retention_rate,
                ROUND(AVG(mrr.retention_rate), 2) AS message_avg_retention_rate
        FROM message_retention_by_days AS mrr
        JOIN feed_retention_by_days AS frr USING(day)
        GROUP BY day
        ORDER BY day
        """
        rr_df = ph.read_clickhouse(query=query_rr, connection=connection)
        return rr_df
    
    @task(retries=3, retry_delay=timedelta(minutes=10))
    def dau_transformator(dau_df):
        # preparing the data for the barplot
        dau_df['message_feed_dau'] = dau_df['feed_dau'] + dau_df['message_dau'] - dau_df['dau']
        avg_dau_percentage_df = pd.DataFrame({'avg_percentage':[
                                             round((dau_df.feed_dau / dau_df.dau).mul(100).mean(), 2),
                                             round((dau_df.message_dau / dau_df.dau).mul(100).mean(), 2),
                                             round((dau_df.message_feed_dau / dau_df.dau).mul(100).mean(), 2)]},
                                             index=['feed_dau',
                                                    'message_dau',
                                                    'message_feed_dau'])
        # making plot and putting into plot object
        dau_plot = io.BytesIO()

        # making the plots
        fig, axis = plt.subplots(1, 2, figsize=(20, 6), gridspec_kw={'width_ratios': [3, 1.75]})
        fig.subplots_adjust(wspace=0.05)

        sns.lineplot(ax=axis[0], x=dau_df.date, y=dau_df.dau, color='red', label='Total DAU')
        sns.lineplot(ax=axis[0], x=dau_df.date, y=dau_df.feed_dau, color='blue', label='Feed DAU')
        sns.lineplot(ax=axis[0], x=dau_df.date, y=dau_df.message_dau, color='green', label='Messenger DAU')

        axis[0].fill_between(dau_df.date, dau_df.dau, color='red', alpha=0.1)
        axis[0].fill_between(dau_df.date, dau_df.feed_dau, color='blue', alpha=0.1)
        axis[0].fill_between(dau_df.date, dau_df.message_dau, color='g', alpha=0.1)

        axis[0].set_title('Daily Active Users', fontsize=16, pad=20)
        axis[0].set_ylabel('')
        axis[0].set_xlabel('Date', fontsize=12)
        axis[0].set_xticks(dau_df.date[::2])
        axis[0].set_xticklabels(dau_df.date[::2].dt.strftime("%m-%d"))
        axis[0].tick_params(axis='x', labelsize=12)
        axis[0].tick_params(axis='y', labelsize=14)
        axis[0].grid(True)
        axis[0].set_ylim(0, dau_df.dau.max() * 1.25)
        axis[0].legend(loc='upper right', fontsize=12)

        sns.barplot(ax=axis[1],
                    x=avg_dau_percentage_df.index,
                    y=avg_dau_percentage_df.avg_percentage)
        axis[1].set_title('Percentage of Total DAU', fontsize=16, pad=20)
        axis[1].set_ylabel('')
        axis[1].set_xlabel('')
        axis[1].set_xticklabels(["Feed DAU\nPercentage",
                                 "Messenger\nDAU Percentage",
                                 "Feed And Messenger\nActive Users"])
        axis[1].set_yticklabels([])
        axis[1].tick_params(axis='x', labelsize=12, length=0)
        axis[1].tick_params(axis='y', length=0)
        axis[1].spines['top'].set_visible(False)
        axis[1].spines['bottom'].set_visible(False)
        axis[1].spines['left'].set_visible(False)
        axis[1].spines['right'].set_visible(False)
        for i, v in enumerate(avg_dau_percentage_df.avg_percentage):
            axis[1].text(i, v + 1, f"{v}%", ha='center', fontsize=14)

        plt.savefig(dau_plot)
        dau_plot.seek(0)
        dau_plot.name = f"dau_plot_{date.today().strftime('%Y_%m_%d')}.png"
        plt.close()
        
        return dau_plot
    
    @task(retries=3, retry_delay=timedelta(minutes=10))
    def rr_transformator(rr_df):
        # preparing the data for the RR on the key days table
        important_days_rr = rr_df.loc[(rr_df.day == 3) | (rr_df.day == 7) | (rr_df.day == 14) | (rr_df.day == 30)]
        important_days_rr = important_days_rr.set_index('day').T
        important_days_rr.iloc[:] = important_days_rr.iloc[:].astype(str)
        important_days_rr = important_days_rr + "%"
        important_days_rr = important_days_rr.reset_index()
        important_days_rr.iloc[0, 0], important_days_rr.iloc[1, 0] = ['Feed RR', 'Messenger RR']
        
        # making plot and putting into plot object
        rr_plot = io.BytesIO()

        # making the plot
        fig, axis = plt.subplots(2, 1, figsize=(10, 7), gridspec_kw={'height_ratios': [3, 1]})
        fig.subplots_adjust(hspace=0.5)

        sns.lineplot(ax=axis[0], x=rr_df.day, y=rr_df.feed_avg_retention_rate, color='blue', label='Feed RR')
        sns.lineplot(ax=axis[0], x=rr_df.day, y=rr_df.message_avg_retention_rate, color='green', label='Message RR')
        
        axis[0].set_title('Average Retention Rate, %', fontsize=16, pad=10)
        axis[0].set_ylabel('')
        axis[0].set_xlabel('Day', fontsize=14)
        axis[0].set_xticks(rr_df.day[1::2])
        axis[0].set_xticklabels(rr_df.day[1::2])
        axis[0].tick_params(axis='x', labelsize=14)
        axis[0].tick_params(axis='y', labelsize=14)
        axis[0].grid(True)
        axis[0].legend(loc='upper right', fontsize=12)
        
        # making the table
        axis[1].axis('tight')  # Removes axes
        axis[1].axis('off')  # Hides background
        table = axis[1].table(cellText=important_days_rr.values,
                              colLabels=['N Day', "3 Day", '7 Day', '14 Day', '30 Day'],
                              cellLoc='center', 
                              loc='center',
                              colWidths=[0.15 for i in range(len(important_days_rr.columns))],
                              colColours=['lightgrey' for i in range(len(important_days_rr.columns))]
                             )
        table.auto_set_font_size(False)
        table.set_fontsize(14)
        table.scale(1.3, 1.8)
        for (i, j), cell in table.get_celld().items():
            if (i, j) == (1, 0) or (i, j) == (2, 0):
                cell.set_facecolor('lightgrey')
        axis[1].set_title('Retention Rate Over Key Days', fontsize=16)

        plt.savefig(rr_plot)
        rr_plot.seek(0)
        rr_plot.name = f"rr_plot_{date.today().strftime('%Y_%m_%d')}.png"
        plt.close()
        
        return rr_plot
    
    @task(retries=3, retry_delay=timedelta(minutes=10))
    def message_loader(dau_plot, rr_plot):
        # preparing the message
        last_day = dau_df.date.max().strftime('%B %d, %Y')
        first_day = dau_df.date.min().strftime('%B %d, %Y')
        message = f"Above, you can see images of key product metrics over time, all calculated based on a 30-day period (from <b>{first_day}</b> to <b>{last_day}</b>):\n\n" \
f"- <b>Daily Active Users (DAU)</b>: This image displays the total DAU, as well as DAU for the Feed and Messenger. Additionally, it shows the average percentage of each DAU type in relation to the total DAU, including users who are active in both sections of the app.\n" \
f"- <b>Retention Rate (RR)</b>: This image presents the average RR over 30 days. A table is also included, showing RR values for key days within the period.\n"

        media = [
            tg.InputMediaPhoto(dau_plot, caption=message, parse_mode="HTML"),
            tg.InputMediaPhoto(rr_plot)
        ]
        
        # sending the message
        my_token = "**********************************************"
        bot = tg.Bot(token=my_token)
        bot.send_media_group(chat_id=*********, media=media)
    
    connection = {
    'host': '*************************************',
    'password': '***************',
    'user': '*******',
    'database': '******************'
    }
    
    # extraction
    dau_df = dau_extractor(connection)
    rr_df = rr_extractor(connection)
    
    # transformation
    dau_plot = dau_transformator(dau_df)
    rr_plot = rr_transformator(rr_df)
    
    #loading
    message_loader(dau_plot, rr_plot)

# dag starter
app_report_sender = app_report_sender()