# Databricks notebook source
# MAGIC %md
# MAGIC ### 💰 fat_vendas
# MAGIC
# MAGIC - PK: 
# MAGIC   - `cd_venda`
# MAGIC
# MAGIC - Sequence By:
# MAGIC   - `ts_load`
# MAGIC
# MAGIC - Descrição: 
# MAGIC   - Métricas das transações.
# MAGIC
# MAGIC - Objetivo: 
# MAGIC | Campo       | Tipo   | Descrição   |
# MAGIC | ----------- | ------ | ----------- |
# MAGIC | cd_venda    | INT    | PK          |
# MAGIC | cd_cliente  | INT    | FK cliente  |
# MAGIC | cd_produto  | INT    | FK produto  |
# MAGIC | cd_data     | INT    | FK data     |
# MAGIC | cd_pais     | INT    | FK país     |
# MAGIC | qt_produto    | INT    | Quantidade  |
# MAGIC | vl_total_venda | DOUBLE | Valor total |
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
    "prefix": "fat",
    "table": "venda",
    ## Origem.
    "orCatalog": "lakeflow",
    "orSchema": "c_silver",
    "orTable": "slv_orders",
    # Informações.
    "PK": "cd_venda",
    "partition": "dt_partition",
    "sequence_by": "ts_load",
    "Descrição": "Métricas das transações."
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
    "cd_venda": {
        "datatype": "integer",
        "description": "Código único da transação."
    },
    "cd_cliente": {
        "datatype": "integer",
        "description": "Código único do Cliente."
    },
    "cd_produto": {
        "datatype": "integer",
        "description": "Código único do Produto"
    },
    "cd_tempo": {
        "datatype": "integer",
        "description": "Código único que faz Ref a Data."
    },
    "cd_pais": {
        "datatype": "integer",
        "description": "Código único do Pais."
    },
    "qt_produto": {
        "datatype": "integer",
        "description": "Quantidade de produtos vendidos."
    },
    "vl_total_venda": {
        "datatype": "double",
        "description": "Valor total da venda."
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

from pyspark.sql.functions import col, max, min, current_date, current_timestamp, monotonically_increasing_id, year, month, dayofmonth
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
# MAGIC ## 02. Leitura de Tabelas Delta

# COMMAND ----------

# MAGIC %md
# MAGIC ## 02. Leitura da Camada Silver

# COMMAND ----------

# Leitura das informações do arquivo CSV na camada Bronze.
if boolean_carga_full == False:
    # Carga incremental: lê apenas as partições mais recentes.
    df_silver = (
        spark.read \
        .table(orPath) \
        .filter(col("dt_partition") >= max_partition)
    )
else:
    # Carga completa: lê todos os pedidos da tabela de origem.
    df_silver = (
        spark.read \
        .table(orPath)
    )

print(f"Quantidade de registros lidos: {df_silver.count()}")
# Exibe o esquema da tabela.
df_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 02.2 Leitura da Camada Gold: `dim_cliente`

# COMMAND ----------

# Define o caminho da tabela de clientes.
path_cliente = f'{dicionario["catalog"]}.{dicionario["schema"]}.dim_cliente'

# Obtém lista de IDs de clientes presentes nos pedidos lidos.
clientes_ids = [row.customer_id for row in df_silver.select("customer_id").distinct().toLocalIterator()]

# Lê a tabela de clientes e filtra apenas os clientes presentes nos pedidos.
df_cliente = (
    spark.read \
    .table(path_cliente) \
    .filter(col("id_cliente").isin(clientes_ids)) \
    .select(
        col("cd_cliente"),
        col("id_cliente")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 02.3 Leitura da Camada Gold: `dim_produto`

# COMMAND ----------

# Define o caminho da tabela de produtos.
path_produto = f'{dicionario["catalog"]}.{dicionario["schema"]}.dim_produto'

# Obtém lista de IDs de produtos presentes nos pedidos lidos.
produtos_ids = [row.product_id for row in df_silver.select("product_id").distinct().toLocalIterator()]

# Lê a tabela de produtos e filtra apenas os clientes presentes nos pedidos.
df_produto = (
    spark.read \
    .table(path_produto) \
    .filter(col("id_produto").isin(produtos_ids)) \
    .select(
        col("cd_produto"),
        col("id_produto")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 02.4 Leitura da Camada Gold: `dim_tempo`

# COMMAND ----------

# Define o caminho da dimensão de tempo.
path_tempo = f'{dicionario["catalog"]}.{dicionario["schema"]}.dim_tempo'

# Obtém a menor data de transação presente nos pedidos.
tempo_id = df_silver.select(min(col("order_date"))).first()[0]

# Lê a tabela de tempo e filtra apenas os registros com datas maiores ou iguais a menor data de transação.
df_tempo = (
    spark.read \
    .table(path_tempo) \
    .filter(col("dt_venda") >= tempo_id) \
    .select(
        col("cd_tempo"),
        col("dt_venda")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 02.5 Leitura da Camada Gold: `dim_pais`

# COMMAND ----------

# Define o caminho da tabela de pais.
path_pais = f'{dicionario["catalog"]}.{dicionario["schema"]}.dim_pais'

# Obtém lista de IDs de pais presentes nos pedidos lidos.
pais_ids = [row.country for row in df_silver.select("country").distinct().toLocalIterator()]

# Lê a tabela de pais e filtra apenas os paises presentes nos pedidos.
df_pais = (
    spark.read \
    .table(path_pais) \
    .filter(col("nm_pais").isin(pais_ids)) \
    .select(
        col("cd_pais"),
        col("nm_pais")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03. Joins

# COMMAND ----------

# Realiza o join dos pedidos com as dimensões de cliente, produto, tempo e país.
df_join = df_silver.alias("S") \
    .join(df_cliente.alias("C"), 
        col("S.customer_id") == col("C.id_cliente"), "left"  # Join com dimensão cliente.
    ) \
    .join(df_produto.alias("P"), 
        col("S.product_id") == col("P.id_produto"), "left"  # Join com dimensão produto.
    ) \
    .join(df_tempo.alias("T"), 
        col("S.order_date") == col("T.dt_venda"), "left"  # Join com dimensão tempo.
    ) \
    .join(df_pais.alias("PA"), 
        col("S.country") == col("PA.nm_pais"), "left"  # Join com dimensão país.
    ) \
    .select(
        col("C.cd_cliente"),  # Código do cliente.
        col("P.cd_produto"),  # Código do produto.
        col("T.cd_tempo"),    # Código do tempo.
        col("PA.cd_pais"),    # Código do país.
        col("S.quantity").alias("qt_produto"),  # Quantidade de produtos vendidos.
        col("S.total_price").alias("vl_total_venda")  # Valor total da venda.
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 05. Adicionando data e hora da carga

# COMMAND ----------

# Adiciona uma coluna com a data de ingestão.
df_ts = (
    df_join \
        .withColumn('dt_partition', current_date())
        .withColumn('ts_load', current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 06. Cria/Atualiza Tabela

# COMMAND ----------

# DBTITLE 1,06. Cria/Atualiza Tabela
if boolean_carga_full:
    # Carga completa: sobrescreve a tabela Silver com todos os dados tratados.
    (
        df_ts \
            .withColumn("cd_venda", monotonically_increasing_id()) \
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
            for col in df_ts.columns if col not in [dicionario['PK'], 'cd_venda']
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