from airflow import DAG

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import boto3

aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')

glue = boto3.client(
        'glue', region_name='sa-east-1',
        aws_access_key_id=aws_access_key_id, 
        aws_secret_access_key=aws_secret_access_key
    )

from airflow.utils.dates import days_ago

def trigger_crawler_regiao():
        glue.start_crawler(Name='pnadconvid19_regiao_crawler')

def trigger_crawler_sexo():
        glue.start_crawler(Name='pnadconvid19_sexo_crawler')


with DAG(
    'pnadconvid19_batch_spark_k8s',
    default_args={
        'owner': 'Anderson',
        'depends_on_past': False,
        'email': ['anderson.theobaldo@gmail.com.br'],
        'email_on_failure': False,
        'email_on_retry': False,
        'max_active_runs': 1,
    },
    description='submit spark-pi as sparkApplication on kubernetes',
    schedule_interval="0 */2 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=['spark', 'kubernetes', 'batch', 'pnadcovid19'],
) as dag:
    converte_parquet = SparkKubernetesOperator(
        task_id='converte_parquet',
        namespace="airflow",
        application_file="converte_parquet.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    converte_parquet_monitor = SparkKubernetesSensor(
        task_id='job_converte_parquet_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='converte_parquet')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )

    agregacao_por_regiao = SparkKubernetesOperator(
        task_id='agregacao_por_regiao',
        namespace="airflow",
        application_file="agregacao_por_regiao.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    agregacao_por_regiao_monitor = SparkKubernetesSensor(
        task_id='agregacao_regiao_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='agregacao_por_regiao')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )

    trigger_crawler_regiao = PythonOperator(
        task_id='trigger_crawler_regiao',
        python_callable=trigger_crawler_regiao,
    )

    agregacao_por_sexo = SparkKubernetesOperator(
        task_id='agregacao_por_sexo',
        namespace="airflow",
        application_file="agregacao_por_sexo.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    agregacao_por_sexo_monitor = SparkKubernetesSensor(
        task_id='agregacao_por_sexo_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='agregacao_por_sexo')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )

    trigger_crawler_sexo = PythonOperator(
        task_id='trigger_crawler_sexo',
        python_callable=trigger_crawler_sexo,
    )

converte_parquet >> converte_parquet_monitor 
converte_parquet_monitor >> agregacao_por_regiao >> agregacao_por_regiao_monitor
converte_parquet_monitor >> agregacao_por_sexo >> agregacao_por_sexo_monitor
agregacao_por_regiao_monitor >> trigger_crawler_regiao
agregacao_por_sexo_monitor >> trigger_crawler_sexo