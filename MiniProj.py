import datetime
import os
from datetime import date, datetime, timedelta

import pandas as pd
import yfinance as yf
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.today(),
    "retries": 2,
    "retry_delay": timedelta(minutes=5)  # retry after 5 minutes
}
dag = DAG(
    "marketvol",
    default_args=default_args,
    description="",
    schedule_interval='0 6 * * 1-5'
)


def download_market_data(stock: str):
    """
    Download market data given stock from yfinance module.
    :param stock (str): stock symbol for company of interest
    ==> DataFrame
    :output: csv
    """
    start_date = date.today()
    end_date = start_date + timedelta(days=1)
    stock_df = yf.download(stock, start=start_date,
                           end=end_date, interval='1m')
    stock_df.to_csv("{}_data.csv".format(stock), header=False)


def max_difference():
    """
    Get maximum difference of a stock price within a given trading day and on what date it occurs.
    """
    # get current working directory
    cwd = os.getcwd()
    for file in cwd:
        # read each csv into a pandas dataframe
        df = pd.read_csv(file)
        # get name of stock from directory name
        stock = cwd.splittext('_')[0]
        idx = (df['high'] - df['low']).abs().idxmax()
        max_diff = df.iloc[idx]['high'] - df.iloc[idx]['low']
        datetm = max_diff['date time'].apply(
            lambda x: datetime.strptime(x, '%d%b%Y:%H:%M:%S.%f'))
        date = datetm.date()

        print("The largest difference in {stock} price is: {}\n".format(stock, max_diff))
        print("On {}".format(date))


# I use oh-my-zsh instead of Bash
# now_template = """
#     var=$(date +"%FORMAT_STRING")
#     now=$(date +"%Y_%m_%d")
#     mkdir -p /tmp/data/$now
# """

bash_template = """
    echo "creating data directory for {{ ds }}"
    mkdir - p /tmp/data/{{ ds }}
"""

# create tmp directory using now_template
t0 = BashOperator(
    task_id='init_temp_directory',
    bash_command=bash_template,  # change it
    dag=dag
)

# download AAPL data
t1 = PythonOperator(
    task_id='get_AAPL_data',
    python_callable=download_market_data,
    op_kwargs={'stock': 'AAPL'},
    dag=dag
)

# download TSLA data
t2 = PythonOperator(
    task_id='get_TSLA_data',
    python_callable=download_market_data,
    op_kwargs={'stock': 'TSLA'},
    dag=dag
)

# move AAPL data to created directory
t3 = BashOperator(
    task_id='store_AAPL_data',
    bash_command='mv -f  AAPL_data.csv /tmp/data/$now',
    dag=dag
)

# move TSLA data to created directory
t4 = BashOperator(
    task_id='store_TSLA_data',
    bash_command='mv -f TSLA_data.csv /tmp/data/$now',
    dag=dag
)
# hadoop: hdfs dfs -put /home/username/file.csv /user/data/file.csv

# run query?
t5 = PythonOperator(
    task_id='max_diff',
    python_callable=max_difference,
    dag=dag
)

# job dependency
t0 >> t1
t0 >> t2
t1 >> t3
t2 >> t4
t3 >> t5
t4 >> t5

