#импорт библиотек для инициализации спарка
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
import findspark
findspark.init()
findspark.find()

#переменные окружения спарка
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

#импорт библиотек для DAG
from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


#задаем все переменные далее по коду (они будут обозначены коментариями в коде где они используются)
sname = "antonbadas"
hdfs_path = "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020"
geo_path = "/user/master/data/geo/events/"
citygeodata_csv = f"{hdfs_path}/user/{sname}/data/citygeodata/"
start_date = '2022-05-21'
depth = '28'




args = {
    "owner": "antonbadas",
    'email': ['antonbadas@yandex.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3
}

with DAG(
        'Calc_metrics_and_exports_marts',
        default_args=args,
        description='Launch pyspark job for update stg layer and call the calculating marts',
        catchup=False,
        schedule_interval=None,
        start_date=datetime(2023, 3, 31),
        tags=['pyspark', 'hdfs'],
        is_paused_upon_creation=True,
) as dag:

    start_task = DummyOperator(task_id='start')

    update_stg_by_date_task = SparkSubmitOperator(
        task_id='update_stg_by_date_job',
        application ='/scripts/update_stg_by_date_job.py',
        conn_id= 'yarn_spark',
        application_args = [sname, hdfs_path, geo_path, start_date, depth],
        conf={
        "spark.driver.maxResultSize": "20g"
        },
        executor_cores = 2,
        executor_memory = '4g'
    )

    calculating_geo_analitics_task = SparkSubmitOperator(
        task_id='calculating_geo_analitics_job',
        application ='/scripts/geo_analytics.py',
        conn_id= 'yarn_spark',
        application_args = [sname, hdfs_path, geo_path, start_date, depth],
        conf={
        "spark.driver.maxResultSize": "20g"
        },
        executor_cores = 2,
        executor_memory = '4g'
    )

    calculating_user_analitics_task = SparkSubmitOperator(
        task_id='calculating_user_analitics_job',
        application ='/scripts/user_analitics.py' ,
        conn_id= 'yarn_spark',
        application_args = [sname, hdfs_path, geo_path, start_date, depth],
        conf={
        "spark.driver.maxResultSize": "20g"
        },
        executor_cores = 4,
        executor_memory = '16g'
    )

    calculating_friend_recomendation_analitics_task = SparkSubmitOperator(
        task_id='calculating_friend_recomendation_analitics_job',
        application ='/scripts/friend_recomendation.py' ,
        conn_id= 'yarn_spark',
        application_args = [sname, hdfs_path, geo_path, start_date, depth],
        conf={
        "spark.driver.maxResultSize": "20g"
        },
        executor_cores = 6,
        executor_memory = '24g'
    )

    end_task = DummyOperator(task_id='end')

    start_task >> update_stg_by_date_task >> calculating_user_analitics_task >> calculating_geo_analitics_task >> calculating_friend_recomendation_analitics_task >> end_task