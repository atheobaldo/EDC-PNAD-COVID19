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
        glue.start_crawler(Name='pnad-convid19-alunos-excluidos-por-regiao')

def trigger_crawler_faixa_etaria():
        glue.start_crawler(Name='pnad-convid19-alunos-excluidos-por-faixa-etaria')

def trigger_crawler_escolaridade():
        glue.start_crawler(Name='pnad-convid19-alunos-excluidos-por-escolaridade')

def trigger_crawler_uf():
        glue.start_crawler(Name='pnad-convid19-alunos-excluidos-por-uf')

def trigger_crawler_tipo_area():
        glue.start_crawler(Name='pnad-convid19-alunos-excluidos-por-tipo-area')

def trigger_crawler_sexo():
        glue.start_crawler(Name='pnad-convid19-alunos-excluidos-por-sexo')

def trigger_crawler_cor():
        glue.start_crawler(Name='pnad-convid19-alunos-excluidos-por-cor')
    

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
        task_id='converte_parquet_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='converte_parquet')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )

    tratamento_de_dados = SparkKubernetesOperator(
        task_id='tratamento_de_dados',
        namespace="airflow",
        application_file="tratamento_de_dados.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    tratamento_de_dados_monitor = SparkKubernetesSensor(
        task_id='tratamento_de_dados_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='tratamento_de_dados')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )

    ###########################################
    ###        Agregação por Região         ###
    ###########################################

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

    ###########################################
    ###     Agregação por Faixa Etaria      ###
    ###########################################
    
    agregacao_por_faixa_etaria = SparkKubernetesOperator(
        task_id='agregacao_por_faixa_etaria',
        namespace="airflow",
        application_file="agregacao_por_faixa_etaria.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    agregacao_por_faixa_etaria_monitor = SparkKubernetesSensor(
        task_id='agregacao_regiao_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='agregacao_por_faixa_etaria')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )

    trigger_crawler_faixa_etaria = PythonOperator(
        task_id='trigger_crawler_faixa_etaria',
        python_callable=trigger_crawler_faixa_etaria,
    )

    ###########################################
    ###     Agregação por escolaridade      ###
    ###########################################
    
    agregacao_por_escolaridade = SparkKubernetesOperator(
        task_id='agregacao_por_escolaridade',
        namespace="airflow",
        application_file="agregacao_por_escolaridade.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    agregacao_por_escolaridade_monitor = SparkKubernetesSensor(
        task_id='agregacao_por_escolaridade_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='agregacao_por_escolaridade')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )

    trigger_crawler_faixa_etaria = PythonOperator(
        task_id='trigger_crawler_faixa_etaria',
        python_callable=trigger_crawler_faixa_etaria,
    )

    ###########################################
    ###        Agregação por estado         ###
    ###########################################
    
    agregacao_por_uf = SparkKubernetesOperator(
        task_id='agregacao_por_uf',
        namespace="airflow",
        application_file="agregacao_por_uf.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    agregacao_por_uf_monitor = SparkKubernetesSensor(
        task_id='agregacao_por_uf_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='agregacao_por_uf')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )

    trigger_crawler_uf = PythonOperator(
        task_id='trigger_crawler_uf',
        python_callable=trigger_crawler_uf,
    )

    ###########################################
    ###       Agregação por tipo area       ###
    ###########################################
    
    agregacao_por_tipo_area = SparkKubernetesOperator(
        task_id='agregacao_por_tipo_area',
        namespace="airflow",
        application_file="agregacao_por_tipo_area.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    agregacao_por_tipo_area_monitor = SparkKubernetesSensor(
        task_id='agregacao_por_tipo_area_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='agregacao_por_tipo_area')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )

    trigger_crawler_tipo_area = PythonOperator(
        task_id='trigger_crawler_tipo_area',
        python_callable=trigger_crawler_tipo_area,
    )

    ###########################################
    ###       Agregação por tipo sexo       ###
    ###########################################
    
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

    ###########################################
    ###         Agregação por cor           ###
    ###########################################
    
    agregacao_por_cor = SparkKubernetesOperator(
        task_id='agregacao_por_cor',
        namespace="airflow",
        application_file="agregacao_por_cor.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    agregacao_por_cor_monitor = SparkKubernetesSensor(
        task_id='agregacao_por_cor_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='agregacao_por_cor')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
    )

    trigger_crawler_cor = PythonOperator(
        task_id='trigger_crawler_cor',
        python_callable=trigger_crawler_cor,
    )


converte_parquet >> converte_parquet_monitor 
converte_parquet_monitor >> tratamento_de_dados >> converte_parquet_monitor
converte_parquet_monitor >> tratamento_de_dados >> tratamento_de_dados_monitor

tratamento_de_dados_monitor >> agregacao_por_regiao >> agregacao_por_regiao_monitor
tratamento_de_dados_monitor >> agregacao_por_faixa_etaria >> agregacao_por_faixa_etaria_monitor
tratamento_de_dados_monitor >> agregacao_por_escolaridade >> agregacao_por_escolaridade_monitor
tratamento_de_dados_monitor >> agregacao_por_uf >> agregacao_por_uf_monitor
tratamento_de_dados_monitor >> agregacao_por_tipo_area >> agregacao_por_tipo_area_monitor
tratamento_de_dados_monitor >> agregacao_por_sexo >> agregacao_por_sexo_monitor
tratamento_de_dados_monitor >> agregacao_por_cor >> agregacao_por_cor_monitor

agregacao_por_regiao_monitor >> trigger_crawler_regiao
agregacao_por_faixa_etaria_monitor >> trigger_crawler_faixa_etaria
agregacao_por_escolaridade_monitor >> trigger_crawler_escolaridade
agregacao_por_uf_monitor >> trigger_crawler_uf
agregacao_por_tipo_area_monitor >> trigger_crawler_tipo_area
agregacao_por_sexo_monitor >> trigger_crawler_sexo
agregacao_por_cor_monitor >> trigger_crawler_cor