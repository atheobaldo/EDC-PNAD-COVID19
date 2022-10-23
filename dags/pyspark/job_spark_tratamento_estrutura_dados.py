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

def renomear_colunas(df_pnad):
    df_pnad = (
        df_pnad
        .withColumnRenamed('Ano'               , 'NU_ANO_REFERENCIA')
        .withColumnRenamed('UF'                , 'CO_UF')
        .withColumnRenamed('CAPITAL'           , 'CO_CAPITAL')
        .withColumnRenamed('RM_RIDE'           , 'CO_REGIAO_METROPOLITANA')
        .withColumnRenamed('V1012'             , 'NU_SEMANA_MES')
        .withColumnRenamed('V1013'             , 'NU_MES')   
        .withColumnRenamed('V1022'             , 'CO_SITUACAO_DOMICILIO')
        .withColumnRenamed('V1023'             , 'CO_TIPO_AREA')
        .withColumnRenamed('A001A'             , 'CO_CONDICAO_DOMICILIO')
        .withColumnRenamed('A001B1'            , 'NU_DIA_NASCIMENTO')
        .withColumnRenamed('A001B2'            , 'NU_MES_NASCIMENTO')
        .withColumnRenamed('A001B3'            , 'NU_ANO_NASCIMENTO')
        .withColumnRenamed('A002'              , 'NU_IDADE_MORADOR')
        .withColumnRenamed('A003'              , 'CO_SEXO')  
        .withColumnRenamed('A004'              , 'CO_COR') 
        .withColumnRenamed('A005'              , 'CO_ESCOLARIDADE') 
        .withColumnRenamed('A006'              , 'CO_FREQUENTA_ESCOLA') 
        .withColumnRenamed('A006A'             , 'CO_TIPO_ESCOLA') 
        .withColumnRenamed('A006B'             , 'CO_AULA_PRESENCIAL') 
        .withColumnRenamed('A007'              , 'CO_TEVE_ATIVIDADE_CASA')  
        .withColumnRenamed('A007A'             , 'CO_MOTIVO_NAO_REALIZOU_ATIVIDADE') 
        .withColumnRenamed('A008'              , 'CO_DIAS_DEDICADO_ATIVIDADE') 
        .withColumnRenamed('A009'              , 'CO_TEMPO_DEDICADO_ATIVIDADE')
        .withColumnRenamed('C011A12'           , 'NU_REMUNERACAO_TOTAL_EM_DINHEIRO') 
        .withColumnRenamed('C011A22'           , 'NU_REMUNERACAO_TOTAL_EM_PRODUTOS')   
    )
    return df_pnad

def ajuste_tipo_de_dado(df_pnad):
    df_pnad = (
        df_pnad
        .withColumn("NU_ANO_REFERENCIA"                           , f.col('NU_ANO_REFERENCIA').cast('int'))
        .withColumn("CO_UF"                                       , f.col('CO_UF').cast('int'))
        .withColumn("CO_CAPITAL"                                  , f.col('CO_CAPITAL').cast('int'))
        .withColumn("CO_REGIAO_METROPOLITANA"                     , f.col('CO_REGIAO_METROPOLITANA').cast('int'))
        .withColumn("NU_SEMANA_MES"                               , f.col('NU_SEMANA_MES').cast('int'))
        .withColumn("NU_MES"                                      , f.col('NU_MES').cast('int'))
        .withColumn("CO_SITUACAO_DOMICILIO"                       , f.col('CO_SITUACAO_DOMICILIO').cast('int'))
        .withColumn("CO_TIPO_AREA"                                , f.col('CO_TIPO_AREA').cast('int'))
        .withColumn("CO_CONDICAO_DOMICILIO"                       , f.col('CO_CONDICAO_DOMICILIO').cast('int'))
        .withColumn("NU_DIA_NASCIMENTO"                           , f.col('NU_DIA_NASCIMENTO').cast('int'))
        .withColumn("NU_MES_NASCIMENTO"                           , f.col('NU_MES_NASCIMENTO').cast('int'))
        .withColumn("NU_ANO_NASCIMENTO"                           , f.col('NU_ANO_NASCIMENTO').cast('int'))
        .withColumn("NU_IDADE_MORADOR"                            , f.col('NU_IDADE_MORADOR').cast('int'))
        .withColumn("CO_SEXO"                                     , f.col('CO_SEXO').cast('int'))
        .withColumn("CO_COR"                                      , f.col('CO_COR').cast('int'))
        .withColumn("CO_ESCOLARIDADE"                             , f.col('CO_ESCOLARIDADE').cast('int')) 
        .withColumn("CO_FREQUENTA_ESCOLA"                         , f.col('CO_FREQUENTA_ESCOLA').cast('int'))
        .withColumn("CO_TIPO_ESCOLA"                              , f.col('CO_TIPO_ESCOLA').cast('int'))
        .withColumn("CO_AULA_PRESENCIAL"                          , f.col('CO_AULA_PRESENCIAL').cast('int'))
        .withColumn("CO_TEVE_ATIVIDADE_CASA"                      , f.col('CO_TEVE_ATIVIDADE_CASA').cast('int'))
        .withColumn("CO_MOTIVO_NAO_REALIZOU_ATIVIDADE"            , f.col('CO_MOTIVO_NAO_REALIZOU_ATIVIDADE').cast('int'))
        .withColumn("CO_DIAS_DEDICADO_ATIVIDADE"                  , f.col('CO_DIAS_DEDICADO_ATIVIDADE').cast('int'))
        .withColumn("CO_TEMPO_DEDICADO_ATIVIDADE"                 , f.col('CO_TEMPO_DEDICADO_ATIVIDADE').cast('int'))
        .withColumn("NU_REMUNERACAO_TOTAL_EM_DINHEIRO"            , f.col('NU_REMUNERACAO_TOTAL_EM_DINHEIRO').cast('decimal'))
        .withColumn("NU_REMUNERACAO_TOTAL_EM_PRODUTOS"            , f.col('NU_REMUNERACAO_TOTAL_EM_PRODUTOS').cast('decimal'))
    )
    return df_pnad

def mapeamento_uf(df_pnad):
    df_pnad = (
        df_pnad
        .withColumn("DESC_UF",  
        f.when(f.col("CO_UF") == 11, 'Rondônia')
        .when(f.col("CO_UF") == 12, 'Acre') 
        .when(f.col("CO_UF") == 13, 'Amazonas')
        .when(f.col("CO_UF") == 14, 'Roraima')
        .when(f.col("CO_UF") == 15, 'Pará')
        .when(f.col("CO_UF") == 16, 'Amapá')
        .when(f.col("CO_UF") == 17, 'Tocantins')
        .when(f.col("CO_UF") == 21, 'Maranhão')
        .when(f.col("CO_UF") == 22, 'Piauí')
        .when(f.col("CO_UF") == 23, 'Ceará')
        .when(f.col("CO_UF") == 24, 'Rio Grande do Norte')
        .when(f.col("CO_UF") == 25, 'Paraíba')
        .when(f.col("CO_UF") == 26, 'Pernambuco')
        .when(f.col("CO_UF") == 27, 'Alagoas')
        .when(f.col("CO_UF") == 28, 'Sergipe')
        .when(f.col("CO_UF") == 29, 'Bahia')
        .when(f.col("CO_UF") == 31, 'Minas Gerais')
        .when(f.col("CO_UF") == 32, 'Espírito Santo')
        .when(f.col("CO_UF") == 33, 'Rio de Janeiro')
        .when(f.col("CO_UF") == 35, 'São Paulo')
        .when(f.col("CO_UF") == 41, 'Paraná')
        .when(f.col("CO_UF") == 42, 'Santa Catarina')
        .when(f.col("CO_UF") == 43, 'Rio Grande do Sul')
        .when(f.col("CO_UF") == 50, 'Mato Grosso do Sul')
        .when(f.col("CO_UF") == 51, 'Mato Grosso')
        .when(f.col("CO_UF") == 52, 'Goiás')
        .when(f.col("CO_UF") == 53, 'Distrito Federal')
    ))
    return df_pnad

def mapeamento_regiao_pais(df_pnad):
    df_pnad = (
        df_pnad
        .withColumn("DESC_REGIAO_PAIS",  
        f.when((f.col("CO_UF") >= 11) &  (f.col("CO_UF") <= 17), 'NORTE')              
         .when((f.col("CO_UF") >= 21) &  (f.col("CO_UF") <= 29), 'NORDESTE')
         .when((f.col("CO_UF") >= 31) &  (f.col("CO_UF") <= 35), 'SUDESTE')
         .when((f.col("CO_UF") >= 41) &  (f.col("CO_UF") <= 43), 'SUL')
         .when((f.col("CO_UF") >= 50) &  (f.col("CO_UF") <= 53), 'CENTRO-OESTE')
    ))
    return df_pnad

def mapeamento_capital(df_pnad):
    df_pnad = (
        df_pnad
        .withColumn("DESC_CAPITAL",  
        f.when(f.col("CO_CAPITAL") == 11, 'Município de Porto Velho (RO)')
        .when(f.col("CO_CAPITAL") == 12, 'Município de Rio Branco (AC)') 
        .when(f.col("CO_CAPITAL") == 13, 'Município de Manaus (AM)')    
        .when(f.col("CO_CAPITAL") == 14, 'Município de Boa Vista (RR)') 
        .when(f.col("CO_CAPITAL") == 15, 'Município de Belém (PA)') 
        .when(f.col("CO_CAPITAL") == 16, 'Município de Macapá (AP)') 
        .when(f.col("CO_CAPITAL") == 17, 'Município de Palmas (TO)')         
        .when(f.col("CO_CAPITAL") == 21, 'Município de São Luís (MA)')    
        .when(f.col("CO_CAPITAL") == 22, 'Município de Teresina (PI)') 
        .when(f.col("CO_CAPITAL") == 23, 'Município de Fortaleza (CE)') 
        .when(f.col("CO_CAPITAL") == 24, 'Município de Natal (RN)')  
        .when(f.col("CO_CAPITAL") == 25, 'Município de João Pessoa (PB)')
        .when(f.col("CO_CAPITAL") == 26, 'Município de Recife (PE)')  
        .when(f.col("CO_CAPITAL") == 27, 'Município de Maceió (AL)')
        .when(f.col("CO_CAPITAL") == 28, 'Município de Aracaju (SE)')  
        .when(f.col("CO_CAPITAL") == 29, 'Município de Salvador (BA)')
        .when(f.col("CO_CAPITAL") == 31, 'Município de Belo Horizonte (MG)')  
        .when(f.col("CO_CAPITAL") == 32, 'Município de Vitória (ES)')
        .when(f.col("CO_CAPITAL") == 33, 'Município de Rio de Janeiro (RJ)')  
        .when(f.col("CO_CAPITAL") == 35, 'Município de São Paulo (SP)')
        .when(f.col("CO_CAPITAL") == 41, 'Município de Curitiba (PR)')  
        .when(f.col("CO_CAPITAL") == 42, 'Município de Florianópolis (SC)')
        .when(f.col("CO_CAPITAL") == 43, 'Município de Porto Alegre (RS)')
        .when(f.col("CO_CAPITAL") == 50, 'Município de Campo Grande (MS)')
        .when(f.col("CO_CAPITAL") == 51, 'Município de Cuiabá (MT)')                
        .when(f.col("CO_CAPITAL") == 52, 'Município de Goiânia (GO)')
        .when(f.col("CO_CAPITAL") == 53, 'Município de Brasília (DF)')   
    ))
    return df_pnad

def mapeamento_regiao_metropolitana(df_pnad):
    df_pnad = (
        df_pnad
        .withColumn("DESC_REGIAO_METROPOLITANA",  
        f.when(f.col("CO_REGIAO_METROPOLITANA") == 13, 'Região Metropolitana de Manaus (AM)')
        .when(f.col("CO_REGIAO_METROPOLITANA") == 15, 'Região Metropolitana de Belém (PA)') 
        .when(f.col("CO_REGIAO_METROPOLITANA") == 16, 'Região Metropolitana de Macapá (AP)') 
        .when(f.col("CO_REGIAO_METROPOLITANA") == 21, 'Região Metropolitana de Grande São Luís (MA)') 
        .when(f.col("CO_REGIAO_METROPOLITANA") == 22, 'Região Administrativa Integrada de Desenvolvimento da Grande Teresina (PI)')     
        .when(f.col("CO_REGIAO_METROPOLITANA") == 23, 'Região Metropolitana de Fortaleza (CE)') 
        .when(f.col("CO_REGIAO_METROPOLITANA") == 24, 'Região Metropolitana de Natal (RN)') 
        .when(f.col("CO_REGIAO_METROPOLITANA") == 25, 'Região Metropolitana de João Pessoa (PB)') 
        .when(f.col("CO_REGIAO_METROPOLITANA") == 26, 'Região Metropolitana de Recife (PE)')  
        .when(f.col("CO_REGIAO_METROPOLITANA") == 27, 'Região Metropolitana de Maceió (AL)') 
        .when(f.col("CO_REGIAO_METROPOLITANA") == 28, 'Região Metropolitana de Aracaju (SE)') 
        .when(f.col("CO_REGIAO_METROPOLITANA") == 29, 'Região Metropolitana de Salvador (BA)') 
        .when(f.col("CO_REGIAO_METROPOLITANA") == 31, 'Região Metropolitana de Belo Horizonte (MG)')     
        .when(f.col("CO_REGIAO_METROPOLITANA") == 32, 'Região Metropolitana de Grande Vitória (ES)') 
        .when(f.col("CO_REGIAO_METROPOLITANA") == 33, 'Região Metropolitana de Rio de Janeiro (RJ)') 
        .when(f.col("CO_REGIAO_METROPOLITANA") == 35, 'Região Metropolitana de São Paulo (SP)') 
        .when(f.col("CO_REGIAO_METROPOLITANA") == 41, 'Região Metropolitana de Curitiba (PR)')  
        .when(f.col("CO_REGIAO_METROPOLITANA") == 42, 'Região Metropolitana de Florianópolis (SC)') 
        .when(f.col("CO_REGIAO_METROPOLITANA") == 43, 'Região Metropolitana de Porto Alegre (RS)')  
        .when(f.col("CO_REGIAO_METROPOLITANA") == 51, 'Região Metropolitana de Vale do Rio Cuiabá (MT)') 
        .when(f.col("CO_REGIAO_METROPOLITANA") == 52, 'Região Metropolitana de Goiânia (GO)')                 
    ))
    return df_pnad

def mapeamento_situacao_domicilio(df_pnad):
    df_pnad = (
        df_pnad
        .withColumn("DESC_SITUACAO_DOMICILIO",  
        f.when(f.col("CO_SITUACAO_DOMICILIO") == 1, 'Urbana')
        .when(f.col("CO_SITUACAO_DOMICILIO") == 2, 'Rural') 
    ))
    return df_pnad

def mapeamento_tipo_area(df_pnad):
    df_pnad = (
        df_pnad
        .withColumn("DESC_TIPO_AREA",  
        f.when(f.col("CO_TIPO_AREA") == 1, 'Capital')
        .when(f.col("CO_TIPO_AREA") == 2, 'Resto da RM (Região Metropolitana, excluindo a capital)') 
        .when(f.col("CO_TIPO_AREA") == 3, 'Resto da RIDE (Região Integrada de Desenvolvimento Econômico, excluindo a capital) ')
        .when(f.col("CO_TIPO_AREA") == 4, 'Resto da UF  (Unidade da Federação, excluindo a região metropolitana e a RIDE)')
    ))
    return df_pnad

def mapeamento_condicao_domicilio(df_pnad):
    df_pnad = (
        df_pnad
        .withColumn("DESC_CONDICAO_DOMICILIO",  
        f.when(f.col("CO_CONDICAO_DOMICILIO") == 1, 'Pessoa responsável pelo domicílio')
        .when(f.col("CO_CONDICAO_DOMICILIO") == 2, 'Cônjuge ou companheiro(a) de sexo diferente') 
        .when(f.col("CO_CONDICAO_DOMICILIO") == 3, 'Cônjuge ou companheiro(a) do mesmo sexo') 
        .when(f.col("CO_CONDICAO_DOMICILIO") == 4, 'Filho(a) do responsável e do cônjuge')  
        .when(f.col("CO_CONDICAO_DOMICILIO") == 5, 'Filho(a) somente do responsável') 
        .when(f.col("CO_CONDICAO_DOMICILIO") == 6, 'Enteado(a)') 
        .when(f.col("CO_CONDICAO_DOMICILIO") == 7, 'Genro ou nora')
        .when(f.col("CO_CONDICAO_DOMICILIO") == 8, 'Pai, mãe, padrasto ou madrasta') 
        .when(f.col("CO_CONDICAO_DOMICILIO") == 9, 'Sogro(a)') 
        .when(f.col("CO_CONDICAO_DOMICILIO") == 10, 'Neto(a)')
        .when(f.col("CO_CONDICAO_DOMICILIO") == 11, 'Bisneto(a)')
        .when(f.col("CO_CONDICAO_DOMICILIO") == 12, 'Irmão ou irmã') 
        .when(f.col("CO_CONDICAO_DOMICILIO") == 13, 'Avô ou avó') 
        .when(f.col("CO_CONDICAO_DOMICILIO") == 14, 'Outro parente')  
        .when(f.col("CO_CONDICAO_DOMICILIO") == 15, 'Agregado(a) - Não parente que não compartilha despesas') 
        .when(f.col("CO_CONDICAO_DOMICILIO") == 16, 'Convivente - Não parente que compartilha despesas') 
        .when(f.col("CO_CONDICAO_DOMICILIO") == 17, 'Pensionista')
        .when(f.col("CO_CONDICAO_DOMICILIO") == 18, 'Empregado(a) doméstico(a)') 
        .when(f.col("CO_CONDICAO_DOMICILIO") == 19, 'Parente do(a) empregado(a) doméstico(a)')
    ))
    return df_pnad

def mapeamento_grupo_idade(df_pnad):
    df_pnad = (
        df_pnad
        .withColumn("NU_GRUPO_IDADE",  
        f.when((f.col("NU_IDADE_MORADOR") >= 4)  & (f.col("NU_IDADE_MORADOR") <= 5), 1)
         .when((f.col("NU_IDADE_MORADOR") >= 6)  & (f.col("NU_IDADE_MORADOR") <= 10), 2) 
         .when((f.col("NU_IDADE_MORADOR") >= 11) & (f.col("NU_IDADE_MORADOR") <= 14), 3)
         .when((f.col("NU_IDADE_MORADOR") >= 15) & (f.col("NU_IDADE_MORADOR") <= 17), 4)
         .when((f.col("NU_IDADE_MORADOR") > 17), 5)
         .otherwise(0)
    ))

def mapeamento_sexo(df_pnad):
    df_pnad = (
        df_pnad
        .withColumn("DESC_SEXO",  
        f.when(f.col("CO_SEXO") == 1, 'Masculino')
        .when(f.col("CO_SEXO") == 2, 'Feminino') 
    ))
    return df_pnad

def mapeamento_cor(df_pnad):
    df_pnad = (
        df_pnad
        .withColumn("DESC_COR",  
        f.when(f.col("CO_COR") == 1, 'Branca')
        .when(f.col("CO_COR") == 2, 'Preta')
        .when(f.col("CO_COR") == 3, 'Amarela')
        .when(f.col("CO_COR") == 4, 'Parda')
        .when(f.col("CO_COR") == 5, 'Indígina')
        .when(f.col("CO_COR") == 9, 'Ignorado')
    ))
    return df_pnad

def mapeamento_escolaridade(df_pnad):
    df_pnad = (
        df_pnad
        .withColumn("DESC_ESCOLARIDADE",  
        f.when(f.col("CO_ESCOLARIDADE") == 1, 'Sem instrução')
        .when(f.col("CO_ESCOLARIDADE") == 2, 'Fundamental incompleto') 
        .when(f.col("CO_ESCOLARIDADE") == 3, 'Fundamental completa')
        .when(f.col("CO_ESCOLARIDADE") == 4, 'Médio incompleto')
        .when(f.col("CO_ESCOLARIDADE") == 5, 'Médio completo')
        .when(f.col("CO_ESCOLARIDADE") == 6, 'Superior incompleto')
        .when(f.col("CO_ESCOLARIDADE") == 7, 'Superior completo')
        .when(f.col("CO_ESCOLARIDADE") == 8, 'Pós-graduação, mestrado ou doutorado')
    ))
    return df_pnad

def mapeamento_frequenta_escola(df_pnad):
    df_pnad = (
        df_pnad
        .withColumn("DESC_FREQUENTA_ESCOLA",  
        f.when(f.col("CO_FREQUENTA_ESCOLA") == 1, 'Sim')
        .when(f.col("CO_FREQUENTA_ESCOLA") == 2, 'Não') 
        .otherwise('Não aplicável')         
    ))
    return df_pnad

def mapeamento_tipo_escola(df_pnad):
    df_pnad = (
        df_pnad
        .withColumn("DESC_TIPO_ESCOLA",  
        f.when(f.col("CO_TIPO_ESCOLA") == 1, 'Pública')
        .when(f.col("CO_TIPO_ESCOLA") == 2, 'Privada') 
        .otherwise('Não aplicável')         
    ))
    return df_pnad

def mapeamento_aula_presencial(df_pnad):
    df_pnad = (
        df_pnad
        .withColumn("DESC_AULA_PRESENCIAL",  
        f.when(f.col("CO_AULA_PRESENCIAL") == 1, 'Sim, normalmente')
        .when(f.col("CO_AULA_PRESENCIAL") == 2, 'Sim, mas apenas parcialmente') 
        .when(f.col("CO_AULA_PRESENCIAL") == 3, 'Não, e meu normalmente é presencial/semipresencial')
        .when(f.col("CO_AULA_PRESENCIAL") == 4, 'Não, meu curso é online')
        .otherwise('Não aplicável')         
    ))    

def mapeamento_teve_atividade_casa(df_pnad):
    df_pnad = (
        df_pnad
        .withColumn("DESC_TEVE_ATIVIDADE_CASA",  
        f.when(f.col("CO_TEVE_ATIVIDADE_CASA") == 1, 'Sim, e realizou pelo menos parte delas')
         .when(f.col("CO_TEVE_ATIVIDADE_CASA") == 2, 'Sim, mas não realizou (por qualquer motivo)') 
         .when(f.col("CO_TEVE_ATIVIDADE_CASA") == 3, 'Não')
         .when(f.col("CO_TEVE_ATIVIDADE_CASA") == 3, 'Não, porque estava de férias')
         .otherwise('Não aplicável')  
    ))
    return df_pnad

def mapeamento_motivo_nao_realizou_atividade(df_pnad):
    df_pnad = (
        df_pnad
        .withColumn("DESC_MOTIVO_NAO_REALIZOU_ATIVIDADE",  
        f.when(f.col("CO_MOTIVO_NAO_REALIZOU_ATIVIDADE") == 1, 'Não tinha computador / tablet / celular disponível')
        .when(f.col("CO_MOTIVO_NAO_REALIZOU_ATIVIDADE") == 2, 'Não tinha acesso à internet ou a qualidade dela era insuficiente') 
        .when(f.col("CO_MOTIVO_NAO_REALIZOU_ATIVIDADE") == 3, 'Por problemas de saúde da própria pessoa')
        .when(f.col("CO_MOTIVO_NAO_REALIZOU_ATIVIDADE") == 4, 'Tinha que cuidar dos afazeres domésticos, do(s) filhos ou de outro(s) parentes')
        .when(f.col("CO_MOTIVO_NAO_REALIZOU_ATIVIDADE") == 5, 'Não conseguiu se concentrar')
        .when(f.col("CO_MOTIVO_NAO_REALIZOU_ATIVIDADE") == 6, 'Outro motivo. Especifique.')
        .otherwise('Não aplicável')  
    ))
    return df_pnad

def mapeamento_dias_dedicacao_atividade(df_pnad):
    df_pnad = (
        df_pnad
        .withColumn("DESC_DIAS_DEDICADO_ATIVIDADE",  
        f.when(f.col("CO_DIAS_DEDICADO_ATIVIDADE") == 1, '1 dia')
         .when(f.col("CO_DIAS_DEDICADO_ATIVIDADE") == 2, '2 dias')
         .when(f.col("CO_DIAS_DEDICADO_ATIVIDADE") == 3, '3 dias')
         .when(f.col("CO_DIAS_DEDICADO_ATIVIDADE") == 4, '4 dias')
         .when(f.col("CO_DIAS_DEDICADO_ATIVIDADE") == 5, '5 dias')
         .when(f.col("CO_DIAS_DEDICADO_ATIVIDADE") == 6, '6 ou 7 dias')
         .otherwise('Não aplicável') 
    ))
    return df_pnad

def mapeamento_tempo_dedicado_atividade(df_pnad):
    df_pnad = (
        df_pnad
        .withColumn("DESC_TEMPO_DEDICADO_ATIVIDADE",  
        f.when(f.col("CO_TEMPO_DEDICADO_ATIVIDADE") == 1, 'Menos de 1 hora')
         .when(f.col("CO_TEMPO_DEDICADO_ATIVIDADE") == 2, 'De 1 hora a menos de 2 horas') 
         .when(f.col("CO_TEMPO_DEDICADO_ATIVIDADE") == 3, 'De 2 horas a menos de 5 horas')
         .when(f.col("CO_TEMPO_DEDICADO_ATIVIDADE") == 4, '5 horas ou mais')
         .otherwise('Não aplicável') 
    ))
    return df_pnad

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
        .load('s3a://igti-datalake-astheobaldo/datalake/bronze/pnad-covid19/')
    )

    print("*************************************************************************")
    print("** Tratamento - nomes de colunas, tipos de dados e mapeamento de dados **")
    print("*************************************************************************")

    df_pnad = renomear_colunas(df_pnad)
    df_pnad = ajuste_tipo_de_dado(df_pnad)
    df_pnad = mapeamento_uf(df_pnad)
    df_pnad = mapeamento_regiao_pais(df_pnad)
    df_pnad = mapeamento_capital(df_pnad)
    df_pnad = mapeamento_regiao_metropolitana(df_pnad)
    df_pnad = mapeamento_situacao_domicilio(df_pnad)
    df_pnad = mapeamento_tipo_area(df_pnad)
    df_pnad = mapeamento_condicao_domicilio(df_pnad)
    df_pnad = mapeamento_grupo_idade(df_pnad)
    df_pnad = mapeamento_sexo(df_pnad)
    df_pnad = mapeamento_cor(df_pnad)
    df_pnad = mapeamento_escolaridade(df_pnad)
    df_pnad = mapeamento_frequenta_escola(df_pnad)
    df_pnad = mapeamento_tipo_escola(df_pnad)
    df_pnad = mapeamento_aula_presencial(df_pnad)
    df_pnad = mapeamento_teve_atividade_casa(df_pnad)
    df_pnad = mapeamento_motivo_nao_realizou_atividade(df_pnad)
    df_pnad = mapeamento_dias_dedicacao_atividade(df_pnad)
    df_pnad = mapeamento_tempo_dedicado_atividade(df_pnad)

    (
        df_pnad
        .write
        .mode("overwrite")
        .format("parquet")
        .save('s3a://igti-datalake-astheobaldo/datalake/silver/pnad-covid19/')
    )

    print("**************************************")
    print("** Tratamento realizao com sucesso! **")
    print("**************************************")

    spark.stop()
    