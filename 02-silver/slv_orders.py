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
    "partition": "dt_partition",
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
    "dt_partition": {
        "datatype": "date",
        "description": "Data de ingestão do registro no Data Lake."
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

from pyspark.sql.functions import col, date_sub, current_date, initcap, round, upper, when, current_date, current_timestamp, row_number
from delta.tables import DeltaTable
from pyspark.sql.window import Window

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
    max_partition = partitions.select(max("dt_partition")).first()[0]
    print(f"Carga incremental a partir da data: {max_partition}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 02. Leitura da Camada Bronze

# COMMAND ----------

# Leitura das informações do arquivo CSV na camada Bronze.
if boolean_carga_full == False:
    # Carga incremental: lê apenas as partições mais recentes.
    df_bronze = (
        spark.read \
        .table(orPath)
        .filter(col("dt_partition") >= max_partition)
    )
else:
    # Carga completa: lê todos os pedidos da tabela de origem.
    df_bronze = (
        spark.read \
        .table(orPath)
    )

print(f"Quantidade de registros lidos: {df_bronze.count()}")
# Exibe o esquema da tabela.
df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03. Tratamento de DataTypes

# COMMAND ----------

# Loop pelas colunas definidas no dicionário de schema.
for field in orders_schema_dict.keys():
    try:
        # Obtém o tipo de dado atual da coluna no DataFrame.
        dsType = df_bronze.schema[field].dataType.simpleString()
    except:
        # Caso a coluna não exista, exibe mensagem de erro e continua.
        print(f"Erro: {field}. Coluna não localizada.")
        continue

    # Obtém o tipo de dado desejado conforme o dicionário de schema.
    dsTypeNew = orders_schema_dict[field]['datatype']

    # Realiza a conversão de tipo conforme especificado.
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
        # Caso o tipo de dado não seja suportado, exibe mensagem de erro.
        print(f"Erro: {field}. O tipo de dados '{dsType}' não é suportado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 04. Tratamento de Colunas especificas

# COMMAND ----------

# Adiciona a coluna 'total_price' calculando o valor total do pedido (quantity × unit_price) arredondado para 2 casas decimais.
df_new_column = df_bronze.withColumn(
    "total_price",
    round(col("quantity") * col("unit_price"), 2))

# COMMAND ----------

# Normaliza o nome do país para formato capitalizado (ex: 'Brasil', 'Estados Unidos').
df_normalize01 = df_new_column.withColumn(
    "country",
    initcap(col("country")))

# Normaliza o status do pedido para letras maiúsculas (ex: 'DELIVERED').
df_normalize02 = df_normalize01.withColumn(
    "status",
    upper(col("status")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 05. Tratamento de nulos

# COMMAND ----------

# DataFrame tratado após normalização.
df_treat = df_normalize02

# Loop para tratar valores nulos conforme o tipo de dado de cada coluna.
for field in df_treat.columns:
    type_ds = df_treat.schema[field].dataType.simpleString()

    # Preenche nulos de colunas string com '#NI'.
    if type_ds == 'string':
        df_treat = df_treat.fillna({field: '#NI'})
    
    # Preenche nulos de colunas integer com -1.
    elif type_ds == 'integer':
        df_treat = df_treat.fillna({field: -1})
        
    # Preenche nulos de colunas double com -1.0.
    elif type_ds == 'double':
        df_treat = df_treat.fillna({field: -1.0})
    
    # Preenche nulos de colunas date com '1900-01-01'.
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
# MAGIC ## 06. Aplicação de filtros

# COMMAND ----------

# DBTITLE 1,Cell 21
# Define a janela para particionar por 'order_id' e ordenar por 'ts_load' decrescente.
window_spec = Window.partitionBy("order_id").orderBy(col("ts_load").desc())

# Seleciona o registro mais recente por 'order_id' e remove colunas técnicas.
df_filterd = (
    df_treat
    .withColumn("row_num", row_number().over(window_spec))  # Adiciona número da linha para cada partição.
    .filter(col("row_num") == 1)
    .drop(*["row_num", "ts_load", "dt_partition"])          # Remove colunas auxiliares.
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 07. Adicionando data e hora da carga

# COMMAND ----------

# Adiciona uma coluna com a data de ingestão.
df_ts = (
    df_filterd \
        .withColumn('dt_partition', current_date())
        .withColumn('ts_load', current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 07. Selecionando Colunas

# COMMAND ----------

# Seleciona apenas as colunas definidas no dicionário de schema para o DataFrame final.
df_select = df_ts.select(*orders_schema_dict.keys())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 08. Cria/Atualiza Tabela

# COMMAND ----------

if boolean_carga_full:
    # Carga completa: sobrescreve a tabela Silver com todos os dados tratados.
    (
        df_select \
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

            
            source=df_select.alias("s"),
            condition=f"t.{dicionario['PK']} = s.{dicionario['PK']}"
        )
        # Realiza atualização das linhas existentes e insere novas linhas.
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