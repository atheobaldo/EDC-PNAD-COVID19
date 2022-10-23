from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col
import pyspark.sql.functions as f

# set conf
conf = (
SparkConf()
    .set("spark.hadoop.fs.s3a.fast.upload", True)
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.EnvironmentVariableCredentialsProvider')
    .set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.3')
)

# apply config
sc = SparkContext(conf=conf).getOrCreate()

def agrupar_por_uf(df_pnad):

    return True

if __name__ == "__main__":

    # init spark session
    spark = SparkSession\
            .builder\
            .appName("PNAD CONVID19 Job")\
            .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df_pnad = (
        spark
        .read
        .format("parquet")
        .load('s3a://igti-datalake-astheobaldo/datalake/processing-zone/')
    )

    print("**************************")
    print("** Agregação - por sexo **")
    print("**************************")

    uf_m = (
        df
        .where("CO_SEXO = 1")
        .groupBy("DESC_UF")
        .agg(count(col("CO_SEXO")).alias("count_m"))
    )

    uf_f = (
        df
        .where("CO_SEXO = 2")
        .groupBy("DESC_UF")
        .agg(count(col("CO_SEXO")).alias("count_f"))
    )

    uf_sexo = uf_m.join(uf_f, on="DESC_UF", how="inner")

    (
        uf_sexo
        .write
        .mode("overwrite")
        .format("parquet")
        .save('s3a://igti-datalake-astheobaldo/datalake/consumer-zone/uf_sexo')
    )

    print("**************************************")
    print("** Tratamento realizao com sucesso! **")
    print("**************************************")

    spark.stop()
    