# Databricks notebook source
# MAGIC %md
# MAGIC #### 📅 Dim_Tempo
# MAGIC
# MAGIC - PK: 
# MAGIC   - `cd_tempo`
# MAGIC
# MAGIC - Sequence By:
# MAGIC   - `ts_load`
# MAGIC
# MAGIC - Descrição: 
# MAGIC   - Datas de venda.
# MAGIC
# MAGIC - Objetivo: 
# MAGIC   | Campo      | Tipo |
# MAGIC   | ---------- | ---- |
# MAGIC   | sk_data    | INT  |
# MAGIC   | order_date | DATE |
# MAGIC   | ano        | INT  |
# MAGIC   | mes        | INT  |
# MAGIC   | dia        | INT  |
# MAGIC
# MAGIC Autor: David Costa

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01. Descrições/Parâmetros

# COMMAND ----------

# Definições de parâmetros e Descrição.
dicionario = {
    ## Destino.
    "catalog": "lakeflow",
    "schema": "d_gold",
    "prefix": "dim",
    "table": "tempo",
    ## Origem.
    "orCatalog": "lakeflow",
    "orSchema": "c_silver",
    "orTable": "slv_orders",
    # Informações.
    "PK": "dt_venda",
    "partition": "dt_partition",
    "sequence_by": "ts_load",
    "Descrição": "Datas de venda."
}

# Cria o caminho de origem dos arquivos.
orPath = f'{dicionario["orCatalog"]}.{dicionario["orSchema"]}.{dicionario["orTable"]}'
print(f"Caminho de origem (Bronze): {orPath}")

# Cria o caminho de destino dos arquivos.
path = f'{dicionario["catalog"]}.{dicionario["schema"]}.{dicionario["prefix"]}_{dicionario["table"]}'
print(f"Caminho de destino (Silver): {path}")

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## 01.2 Dicionário de Colunas

# COMMAND ----------

# Definições de parâmetros e Descrição.
orders_schema_dict = {
    "cd_tempo": {
        "datatype": "integer",
        "description": "Código único do tempo."
    },          
    "dt_venda": {
        "datatype": "date",
        "description": "Data historico de vendas."
    },          
    "dt_ano_ref": {
        "datatype": "integer",
        "description": "Ano referencia da venda."
    },
    "dt_mes_ref": {
        "datatype": "integer",
        "description": "Mês referencia da venda."
    },
    "dt_dia_ref": {
        "datatype": "integer",
        "description": "Dia referencia da venda."
    },
    "dt_partition": {
        "datatype": "Date",
        "description": "Data de partição."
    },
    "ts_load": {
        "datatype": "Timestamp",
        "description": "Data e hora de ingestão do registro no Data Lake."
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01.3 Importação de Bibliotecas

# COMMAND ----------

from pyspark.sql.functions import col, max, current_date, current_timestamp, monotonically_increasing_id, year, month, dayofmonth
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01.4 Configurações de Ambientes

# COMMAND ----------

# Verifica se a tabela de origem existe.
if not spark.catalog.tableExists(orPath):
    dbutils.notebook.exit("Execução abortada: Tabela origem não localizada.")

# COMMAND ----------

# Define se a carga será completa ou incremental.
boolean_carga_full = True

# Verifica se a tabela de destino já existe. Se existir, realiza carga incremental.
if spark.catalog.tableExists(path):
    boolean_carga_full = False

    # Retorna uma lista de objetos de partição
    partitions = spark.sql(f"SHOW PARTITIONS {path}")

    # Obtém a data da última partição criada.
    max_partition = partitions.select(max(dicionario["partition"])).first()[0]
    print(f"Carga incremental a partir da data: {max_partition}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 02. Leitura da Camada Silver

# COMMAND ----------

# DBTITLE 1,Cell 12
# Leitura das informações do arquivo CSV na camada Bronze.
if boolean_carga_full == False:
    # Carga incremental: lê apenas as partições mais recentes.
    df_silver = (
        spark.read \
        .table(orPath) \
        .filter(col("dt_partition") >= max_partition) \
        .select(
            col("order_date").alias("dt_venda"),
        ).orderBy(col("order_date").asc())
    )
else:
    # Carga completa: lê todos os pedidos da tabela de origem.
    df_silver = (
        spark.read \
        .table(orPath) \
        .select(
            col("order_date").alias("dt_venda"),
        ).orderBy(col("order_date").asc())
    )

print(f"Quantidade de registros lidos: {df_silver.count()}")
# Exibe o esquema da tabela.
df_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03. Aplicação de filtros

# COMMAND ----------

# Remove duplicatas.
df_dropDuplicates = df_silver.dropDuplicates(['dt_venda'])
print(f"Quantidade de registros após remoção de duplicatas: {df_dropDuplicates.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 04. Tratamento de campos

# COMMAND ----------

# Adicionando colunas com ano, mês e dia referência.
df_treat = (
    df_dropDuplicates
    .withColumn("dt_ano_ref", year(col("dt_venda")))
    .withColumn("dt_mes_ref", month(col("dt_venda")))
    .withColumn("dt_dia_ref", dayofmonth(col("dt_venda")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 05. Adicionando data e hora da carga

# COMMAND ----------

# Adiciona uma coluna com a data de ingestão.
df_ts = (
    df_treat \
        .withColumn('dt_partition', current_date())
        .withColumn('ts_load', current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 06. Cria/Atualiza Tabela

# COMMAND ----------

if boolean_carga_full:
    # Carga completa: sobrescreve a tabela Silver com todos os dados tratados.
    (
        df_ts \
            .withColumn("cd_tempo", monotonically_increasing_id()) \
            .write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy(dicionario['partition']) \
            .saveAsTable(path)
    )

else:
    # Carga incremental: realiza merge (upsert) dos dados tratados na tabela Silver existente.
    delta_table = DeltaTable.forName(spark, path)

    (
        delta_table.alias("t")
        .merge(
            source=df_ts.alias("s"),
            condition=f"t.{dicionario['PK']} = s.{dicionario['PK']}"
        )
        # Realiza atualização das linhas existentes e insere novas linhas.
        .whenMatchedUpdate(set={
            col: f"s.{col}" 
            for col in df_ts.columns if col not in [dicionario['PK'], 'cd_tempo']
        })
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 07. Adiciona Comentarios

# COMMAND ----------

# MAGIC %md
# MAGIC ## 07.1 Adiciona Comentarios: Tabela

# COMMAND ----------

# Adiciona as descrição na Tabela.    
spark.sql(f"COMMENT ON TABLE {path} IS '{dicionario['Descrição']}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 07.2 Adiciona Comentarios: Colunas

# COMMAND ----------

# Adiciona as descrição na Tabela.
if len(orders_schema_dict):
    for field in orders_schema_dict:
        # Guarda a descrição da coluna.
        description = orders_schema_dict[field]['description']
        # Cria o comando SQL para adicionar a descrição.
        sql_cd = f"ALTER TABLE {path} ALTER COLUMN {field} COMMENT '{description}'"
        # Executa o comando SQL.
        spark.sql(sql_cd)