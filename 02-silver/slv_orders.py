# Databricks notebook source
# MAGIC %md
# MAGIC ## 00. slv_orders
# MAGIC
# MAGIC - PK: 
# MAGIC   - `order_id`
# MAGIC
# MAGIC - Sequence By:
# MAGIC   - `order_date`
# MAGIC
# MAGIC - Descrição: 
# MAGIC   - Dados tratados e padronizados, com tipos corrigidos, campos calculados e registros inválidos filtrados.
# MAGIC
# MAGIC - Objetivo: 
# MAGIC   - Limpezas e enriquecimentos:
# MAGIC     - Converter tipos (`quantity` → int, `unit_price` → float).
# MAGIC     - Normalizar `country` e `status`.
# MAGIC     - Criar campo `total_price = quantity * unit_price`.
# MAGIC     - Filtrar apenas `status = 'Delivered'`.
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
    "schema": "c_silver",
    "prefix": "slv",
    "table": "orders",
    ## Origem.
    "orCatalog": "lakeflow",
    "orSchema": "b_bronze",
    "orTable": "ing_orders",
    # Informações.
    "PK": "order_id",
    "partition": "dt_ingestion",
    "sequence_by": "order_date",
    "Descrição": "Dados tratados e padronizados, com tipos corrigidos, campos calculados e registros inválidos filtrados."
}

# Cria o caminho de origem dos arquivos.
orPath = f'{dicionario['orCatalog']}.{dicionario['orSchema']}.{dicionario['orTable']}'
print(f"Caminho de origem (Bronze): {orPath}")

# Cria o caminho de destino dos arquivos.
path = f'{dicionario['catalog']}.{dicionario['schema']}.{dicionario['prefix']}_{dicionario['table']}'
print(f"Caminho de destino (Silver): {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01.2 Dicionário de Colunas

# COMMAND ----------

# Definições de parâmetros e Descrição.
orders_schema_dict = {
    "order_id": {
        "datatype": "integer",
        "description": "Identificador único do pedido."
    },          
    "order_date": {
        "datatype": "date",
        "description": "Data da realização do pedido."
    },
    "customer_id": {
        "datatype": "string",
        "description": "Identificador do cliente."
    },
    "product_id": {
        "datatype": "string",
        "description": "Identificador do produto."
    },      
    "quantity": {
        "datatype": "integer",
        "description": "Quantidade de produtos comprados."
    },
    "unit_price": {
        "datatype": "double",
        "description": "Preço unitário do produto."
    },
    "total_price": {
        "datatype": "double",
        "description": "Valor total do pedido (quantity × unit_price)."
    },
    "country": {
        "datatype": "string",
        "description": "Nome padronizado do país em letras maiúsculas."
    },
    "payment_method": {
        "datatype": "string",
        "description": "Método de pagamento utilizado."
    },
    "status": {
        "datatype": "string",
        "description": "Status do pedido. Somente registros com Delivered são mantidos."
    },
    "ts_carga": {
        "datatype": "timestamp",
        "description": "Data e hora da última atualização do registro."
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01.3 Importação de Bibliotecas

# COMMAND ----------

from pyspark.sql.functions import col, date_sub, current_date, initcap, round, upper, when, current_timestamp
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01.4 Configurações de Ambientes

# COMMAND ----------

# DBTITLE 1,Cell 9
# Retorna uma lista de objetos de partição
partitions = spark.sql("SHOW PARTITIONS lakeflow.b_bronze.ing_orders").collect()

display(partitions)

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
    print(f"Carga incremental.")
else:
    print(f"Carga completa.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 02. Leitura da Camada Bronze

# COMMAND ----------

# Leitura das informações do arquivo CSV na camada Bronze.
if boolean_carga_full == False:
    # Carga incremental: lê apenas pedidos dos últimos 3 dias.
    df_bronze = (
        spark.read \
        .table(orPath)
        .filter(col("order_date").cast("date") >= date_sub(current_date(), 3))
    )
else:
    # Carga completa: lê todos os pedidos da tabela de origem.
    df_bronze = (
        spark.read \
        .table(orPath)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03. Tratamento de DataTypes

# COMMAND ----------

for field in orders_schema_dict.keys():
    try:
        dsType = df_bronze.schema[field].dataType.simpleString()
    except:
        print(f"Erro: {field}. Coluna não localizada.")
        continue

    dsTypeNew = orders_schema_dict[field]['datatype']

    if dsTypeNew == "string":
        df_bronze = df_bronze.withColumn(
            field,
            col(field).cast("string"))
        
        print(f"Coluna '{field}' convertida de '{dsType}' para String.")
    
    elif dsTypeNew == "integer":
        df_bronze = df_bronze.withColumn(
            field,
            col(field).cast("integer"))
        
        print(f"Coluna '{field}' convertida de '{dsType}' para Integer.")
    
    elif dsTypeNew == "double":
        df_bronze = df_bronze.withColumn(
            field,
            col(field).cast("double"))
        
        print(f"Coluna '{field}' convertida de '{dsType}' para Double.")
    
    elif dsTypeNew == "date":
        df_bronze = df_bronze.withColumn(
            field,
            col(field).cast("date"))
        
        print(f"Coluna '{field}' convertida de '{dsType}' para Date.")
    
    else: 
        print(f"Erro: {field}. O tipo de dados '{dsType}' não é suportado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 04. Tratamento de Colunas especificas

# COMMAND ----------

df_new_column = df_bronze.withColumn(
    "total_price",
    round(col("quantity") * col("unit_price"), 2))

# COMMAND ----------

df_normalize01 = df_new_column.withColumn(
    "country",
    initcap(col("country")))

df_normalize02 = df_normalize01.withColumn(
    "status",
    upper(col("status")))

# COMMAND ----------

df_filter_status = df_normalize02.filter(col("status") == "DELIVERED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 05. Tratamento de nulos

# COMMAND ----------

df_treat = df_filter_status

for field in df_treat.columns:
    type_ds = df_treat.schema[field].dataType.simpleString()

    if type_ds == 'string':
        df_treat = df_treat.fillna({field: '#NI'})
    
    elif type_ds == 'integer':
        df_treat = df_treat.fillna({field: -1})
        
    elif type_ds == 'double':
        df_treat = df_treat.fillna({field: -1.0})
    
    elif type_ds == 'date':
        df_treat = df_treat.withColumn(
            field,
            when(
                col(field).isNull(),
                '1900-01-01'
            ).otherwise(col(field))
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 06. Adicionando data e hora da carga

# COMMAND ----------

df_ts = df_treat.withColumn("ts_carga", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 07. Seleção das colunas

# COMMAND ----------

df_select = df_ts.select(*orders_schema_dict.keys())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 08. Cria/Atualiza Tabela

# COMMAND ----------

# Grava os dados no Formato Delta na camada Silver, realizando Merge (Up/Insert) de dados.
from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, path)

(
    delta_table.alias("t")
    .merge(
        source=df_select.alias("s"),
        condition=f"t.{dicionario['PK']} = s.{dicionario['PK']}"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 09. Adiciona Comentarios

# COMMAND ----------

# MAGIC %md
# MAGIC ## 09.1 Adiciona Comentarios: Tabela

# COMMAND ----------

# Adiciona as descrição na Tabela.    
spark.sql(f"COMMENT ON TABLE {path} IS '{dicionario['Descrição']}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 09.2 Adiciona Comentarios: Colunas

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

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.`02silver`.slv_orders