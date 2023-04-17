import os
import findspark
findspark.init()
findspark.find()
from pyspark.sql import SparkSession

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

from datetime import datetime, timedelta
import sys


def main():
    
    sname = sys.argv[1] #"antonbadas" 
    hdfs_path = sys.argv[2] #"hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020"
    geo_path = sys.argv[3]  #"/user/master/data/geo/events/"
    

    spark = (
                SparkSession
                .builder
                .master('yarn')
                .appName(f"{sname}_initial_load")
                .getOrCreate()
            )
    
    #Читаем данные по событиям
    events = spark.read\
                .option("basePath", f"{hdfs_path}{geo_path}")\
                .parquet(f"{hdfs_path}{geo_path}")
    
    #Сохраняю данные сгруппированные по 'date' и 'event_type' для облегчения работы с датафреймом   
    events.write \
            .partitionBy("date", "event_type") \
            .mode("overwrite") \
            .parquet(f"{hdfs_path}/user/{sname}/data/events")
    
    if __name__ == '__main__':
        main()

#Справочник geo.csv залит однократно 
# ! hdfs dfs -put geo.csv user/antonbadas/data/citygeodata
                