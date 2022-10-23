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

    print("**********************************")
    print("** Agregação  por escolaridade  **")
    print("**********************************")

    df_escolaridade = (
        df_pnad
        .where("CO_FREQUENTA_ESCOLA == 2 and NU_GRUPO_IDADE > -1")
        .groupBy("DESC_ESCOLARIDADE")
        .agg(f.count(col("DESC_ESCOLARIDADE")).alias("COUNT"))
    )

    (
        df_escolaridade
        .write
        .mode("overwrite")
        .format("parquet")
        .save('s3a://igti-datalake-astheobaldo/datalake/consumer-zone/alunos-excluidos-por-escolaridade')
    )

    print("**************************************")
    print("** Tratamento realizao com sucesso! **")
    print("**************************************")

    spark.stop()
    