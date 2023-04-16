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
import pyspark.sql.functions as F 
from pyspark.sql.window import Window 
from pyspark.sql.types import *

#задаем все переменные далее по коду они будут обозначены где они используются
sname = sys.argv[1] 
hdfs_path = sys.argv[2] 
geo_path = sys.argv[3]  
citygeodata_csv = f"{hdfs_path}/user/{sname}/data/citygeodata/geo.csv"
start_date = sys.argv[4] 
depth = sys.argv[5] 


#Функция расчета партиционорования данных за день и сохранения в STG слой

def parquet_event(start_date: str, depth: int, sname: str, hdfs_path: str, geo_path: str): 

    for i in range(int(depth)):
        i_date = ((datetime.strptime(start_date, '%Y-%m-%d') - timedelta(days=i)).strftime('%Y-%m-%d'))
        i_input_source_path = hdfs_path + geo_path + "date=" + i_date
        i_output_path = hdfs_path + "/user/" + sname + "/data/events/date=" + i_date
        
        print(f"input: {i_input_source_path}")
        print(f"output: {i_output_path}")
        #Читаем только нужный нам день
        events = (
            spark.read
            .option('basePath', f'{i_input_source_path}')
            .parquet(f"{i_input_source_path}")
        )
        
        #Сохраняем файл в parquet по партиции event_type только в папку соответствующего дня
        events.write.mode('overwrite').partitionBy('event_type').parquet(f'{hdfs_path}/user/{sname}/data/events/date={i_date}')


def main():
    #comment: Create SparkSession
    spark = (
            SparkSession
            .builder
            .master('yarn')
            .appName(f"{sname}_update_stg_by_date_{start_date}_with_depth_{depth}")
            .getOrCreate()
        )

    parquet_event(date=start_date, depth=depth, sname=sname, hdfs_path=hdfs_path, geo_path=geo_path)


if __name__ == '__main__':
    main()