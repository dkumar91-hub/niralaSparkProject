
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder \
                    .master('local[1]') \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()
vaccination = "C:/Users/dilip/Desktop/niralaSparkProject/data/vaccination-data.csv"
v_metaData = "C:/Users/dilip/Desktop/niralaSparkProject/data/vaccination-metadata.csv"
v_whoGbl = "C:/Users/dilip/Desktop/niralaSparkProject/data/vaccination-metadata.csv"
vaccination_df=spark.read.format("csv").option("header","true").load(vaccination)
v_metaDatadf=spark.read.format("csv").option("header","true").load(v_metaData)
v_whoGbld=spark.read.format("csv").option("header","true").load(v_whoGbl)
vaccination_df.show()
v_metaDatadf.show()
v_whoGbld.show()

