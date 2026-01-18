# Databricks notebook source
# MAGIC %md
# MAGIC ## 00. ing_orders
# MAGIC
# MAGIC - PK: 
# MAGIC   - `order_id`
# MAGIC
# MAGIC - Partition:
# MAGIC   - `dt_ingestion`
# MAGIC
# MAGIC - Descrição: 
# MAGIC   - Tabela bruta com os dados originais de pedidos, diretamente ingeridos do arquivo CSV sem transformações.
# MAGIC
# MAGIC - Objetivo: 
# MAGIC   - Leitura direta do CSV.
# MAGIC   - Armazenar em formato **Delta** (`ing_orders`).
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
    "schema": "b_bronze",
    "prefix": "ing",
    "table": "orders",
    ## Origem.
    "orCatalog": "lakeflow",
    "orSchema": "a_raw",
    "orFolder": "medallion-layer-databricks",
    "orFile": "orders_raw.csv",
    # Informações.
    "PK": "order_id",
    "partition": "dt_ingestion",
    "Descrição": "Tabela bruta com os dados originais de pedidos, diretamente ingeridos do arquivo CSV sem transformações."
}

# Cria o caminho de origem dos arquivos.
orPath = f'/Volumes/{dicionario['orCatalog']}/{dicionario['orSchema']}/{dicionario['orFolder']}/'
print(f"Caminho de origem dos arquivos: {orPath}")

# Cria o caminho de destino.
path = f'{dicionario['catalog']}.{dicionario['schema']}.{dicionario['prefix']}_{dicionario['table']}'
print(f"Caminho de destino: {orPath}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01.2 Dicionário de Colunas

# COMMAND ----------

# Definições de parâmetros e Descrição.
orders_schema_dict = {
    "order_id": {
        "datatype": "Int",
        "description": "Identificador único do pedido no sistema de origem."
    },          
    "order_date": {
        "datatype": "Date",
        "description": "Data em que o pedido foi realizado."
    },
    "customer_id": {
        "datatype": "String",
        "description": "Código identificador do cliente que fez o pedido."
    },
    "product_id": {
        "datatype": "String",
        "description": "Código identificador do produto comprado."
    },      
    "quantity": {
        "datatype": "String",
        "description": "Quantidade de produtos comprados (armazenada como texto na origem)."
    },
    "unit_price": {
        "datatype": "String",
        "description": "Preço unitário do produto no momento da compra (texto na origem)."
    },
    "country": {
        "datatype": "String",
        "description": "País onde o pedido foi realizado."
    },
    "payment_method": {
        "datatype": "String",
        "description": "Forma de pagamento utilizada (ex: cartão, PIX, PayPal)."
    },
    "status": {
        "datatype": "String",
        "description": "Situação do pedido no sistema (Delivered, Cancelled, Returned, etc)."
    },
    "dt_ingestion": {
        "datatype": "Date",
        "description": "Data de ingestão do registro no Data Lake."
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01.3 Importação de Bibliotecas

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import col, to_timestamp, to_date, current_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01.4 Configurações de Ambientes

# COMMAND ----------

# DBTITLE 1,Cell 8
# Define se a carga será completa ou incremental.
boolean_carga_full = True

# Verifica se a tabela de destino já existe. Se existir, realiza carga incremental.
if spark.catalog.tableExists(path):
    boolean_carga_full = False

    # Retorna uma lista de objetos de partição
    partitions = spark.sql(f"SHOW PARTITIONS {path}").collect()
    # Obtém a data da última partição criada.
    max_partition = max([p[dicionario['partition']] for p in partitions])
    print(f"Carga incremental a partir da data: {max_partition}")

else:
    print(f"Carga completa.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 02. Leitura dos Arquivos

# COMMAND ----------

# Define o esquema da tabela.
schema = StructType([
    StructField("path", StringType(), True),
    StructField("name", StringType(), True),
    StructField("size", LongType(), True),
    StructField("modificationTime", LongType(), True)
])

try:
    # Lê os arquivos do diretório.
    valores = dbutils.fs.ls(orPath)

    if len(valores) == 0:
        raise Exception("Diretório vazio.")

    else:
        df = spark.createDataFrame(valores, schema) # Cria um DataFrame com os arquivos do diretório.
        print(f"Quantidade de arquivos a serem lidos: ")
        df_01 = (
            df
            .withColumn('modificationDate', to_date(to_timestamp(col('modificationTime') / 1000))) # Converte o Long para data.
            .filter(col('modificationDate') >= max_partition) # Filtro para carregar somente os arquivos mais recentes.
            .select(
                col("name"),
                col("modificationDate")
            )
        )
        display(df_01)
        # Obtém uma lista de nomes de arquivos.
        list_files = [row['name'] for row in df_01.collect()]

        # Passando a lista de caminhos completos diretamente
        full_paths = [orPath + f for f in list_files]

        # Leitura das informações dos arquivos CSV na camada Bronze.
        df_bronze = spark.read \
            .option('header', 'true') \
            .option('sep', ",") \
            .csv(full_paths)
        
except Exception as e:
    dbutils.notebook.exit(f"Ocorreu um erro ao ler o diretório: {orPath}.")

# COMMAND ----------

print(f"Quantidade de registros lidos: {df_bronze.count()}")
# Exibe o esquema da tabela.
df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03. Transformação dos Dados

# COMMAND ----------

# Adiciona uma coluna com a data de ingestão.
df_bronze01 = (
    df_bronze.withColumn('dt_ingestion', current_date())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 04. Cria/Atualiza Tabela

# COMMAND ----------

# Grava os dados no Formato Delta na camada Bronze, realizando só inserção de dados.
(
  df_bronze01
  .write
  .format("delta")
  .mode("append")
  .partitionBy(dicionario["partition"])
  .saveAsTable(path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 05. Adiciona Comentarios

# COMMAND ----------

# MAGIC %md
# MAGIC ## 05.1 Adiciona Comentarios: Tabela

# COMMAND ----------

# Adiciona as descrição na Tabela.    
spark.sql(f"COMMENT ON TABLE {path} IS '{dicionario['Descrição']}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 05.2 Adiciona Comentarios: Colunas

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