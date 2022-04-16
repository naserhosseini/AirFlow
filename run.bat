docker run --platform linux/amd64 -d -p 8081:8081 -v User/mallory/airflow/dags/ puckel/docker-airflow webserver
python3 log_analyzer.py marketvol