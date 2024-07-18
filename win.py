from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark=SparkSession.builder.appName('read_data_from_bq_load_to_bq_stage').master('local[*]').getOrCreate()
df=spark.read.format('csv').\
    option('header',True).\
    load("C:/Users/kesha/Downloads/world-cup-2022/world-cup-2022/wc_matches.csv")


df.withColumn('date',trim(col('date')))\
  .withColumn('league_id',trim(col('league_id')))\
  .withColumn('league',trim(col('league')))\
  .withColumn('team1',trim(col('team1')))\
  .withColumn('team2',trim(col('team2')))\
  .withColumn('spi1',trim(col('spi1')))\
  .withColumn('spi2',trim(col('spi2')))\
  .withColumn('prob1',trim(col('prob1')))\
  .withColumn('prob2',trim(col('prob2')))\
  .withColumn('probtie',trim(col('probtie')))\
  .withColumn('proj_score1',trim(col('proj_score1')))\
  .withColumn('proj_score2',trim(col('proj_score2')))\
  .withColumn('score1',trim(col('score1')))\
  .withColumn('score2',trim(col('score2')))\
  .withColumn('xg1',trim(col('xg1')))\
  .withColumn('xg2',trim(col('xg2')))\
  .withColumn('nsxg1',trim(col('nsxg1')))\
  .withColumn('nsxg2',trim(col('nsxg2')))\
  .withColumn('adj_score1',trim(col('adj_score1')))\
  .withColumn('adj_score2',trim(col('adj_score2'))).show(vertical=True,truncate=False)

