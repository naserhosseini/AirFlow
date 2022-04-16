"""Log analyzer for Airflow workflows"""

from pathlib import Path
import glob

from airflow.operators.python import PythonOperator

from project import *


def list_files(log_dir):
    """
    Recursively list files in a given log directory.
    :param log_dir (str): project directory that contains log files of interest, for example 'marketvol'
    """
    files = []
    # airflow log directory
    base_dir = f'/Users/mallory/airflow/logs/{log_dir}'
    # iterate through directory to list log files
    file_list = Path(base_dir).rglob('*.log')
    # print each log file in list
    for file in file_list:
        files.append(file)
    return files


def analyze_file(log_dir, stock):
    """Parse each log file.
    :param file: log file
    :return err_count: total count of error entries in file
    :return err_list: list of error messages"""
    err_count = 0
    err_list = []

    try:
        # call list_files() function
        # print("calling list_files()...")
        files = list_files(log_dir)
        print("checking for files...")
        for file in files:
            # check for specific stock logs
            # for glob.glob(f"*{stock}*"):
                # print("checking for specified stock...")
                with open(file, 'r') as f:
                    lines = f.readlines()
                    # print("reading file lines...")
                    for line in lines:
                        if 'ERROR' in line:
                            err_count += 1
                            err_list.append(line.split('}} ')[-1])
                            # err_list.append(line)
    # return err_count, err_list
    except:
        if err_count < 1:
            print('Nice–no errors!')
    else:
        print(err_count)
        print(err_list)


t1 = PythonOperator(
    task_id='analyze_AAPL_logs',
    python_callable=analyze_file,
    #dag='marketvol',
    dag=dag,
    op_kwargs={
        'log_dir':'marketvol',
        'stock':'AAPL'
    }
)

t2 = PythonOperator(
    task_id='analyze_TSLA_logs',
    python_callable=analyze_file,
    #dag='marketvol',
    dag=dag,
    op_kwargs={
        'log_dir':'marketvol',
        'stock':'TSLA'
    }
)