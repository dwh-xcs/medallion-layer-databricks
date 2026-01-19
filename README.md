# 🚀 LakeFlow: Medallion Architecture no Databricks (Batch)

Este projeto implementa um pipeline de dados utilizando a **Arquitetura Medalhão** (Bronze, Silver e Gold) dentro da plataforma Databricks. 
O objetivo é transformar dados brutos de vendas em um modelo dimensional (Star Schema) otimizado para análise e BI.

## 🏗️ Arquitetura do Projeto

O pipeline foi desenhado seguindo as melhores práticas de Engenharia de Dados, garantindo idempotência e suporte a cargas incrementais via **Delta Lake**.

![Fluxo de Dados - Arquitetura Medalhão](LINK_DA_SUA_IMAGEM_AQUI)
*Exemplo do fluxo: Bronze (Raw) ➡️ Silver (Standardized) ➡️ Gold (Business)*

### Camadas:
1.  **Bronze (Ingestion):** Ingestão direta do arquivo `orders_raw.csv` para tabelas Delta, preservando a fidelidade dos dados originais e adicionando metadados de controle (`ts_load`).
2.  **Silver (Cleanse & Standardize):** Limpeza de dados, tratamento de tipos, normalização de strings e cálculo de métricas básicas (ex: `total_price`).
3.  **Gold (Dimensional Modeling):** Estruturação em **Star Schema** com Surrogate Keys para facilitar o consumo analítico.

## 🛠️ Tecnologias Utilizadas

* **Linguagem:** Python (PySpark)
* **Plataforma:** Databricks (Unity Catalog)
* **Formato de Armazenamento:** Delta Lake (Transações ACID)
* **Orquestração:** Databricks Workflows (via YAML/DABs)

## 📂 Organização do Repositório

```text
├── 01-bronze/           # Scripts de ingestão inicial
├── 02-silver/           # Scripts de refinamento e limpeza
├── 03-gold/             # Modelagem de Dimensões e Fatos
├── resources/           # Configurações de Job e Infra (YAML)
└── data/                # Sample de dados brutos

```

## ⚙️ Orquestração e Workflow

A inteligência da orquestração está no arquivo `medallion-layer-databricks.yaml`. O Job gerencia dependências automaticamente: as Dimensões são processadas em paralelo após a Silver, e a Fato aguarda a conclusão das Dimensões para garantir integridade.

*Visualização das tarefas e dependências no Databricks Workflows.*

## 📊 Modelo Dimensional (Gold)

O modelo final na camada Gold é composto por:

| Tabela | Tipo | Descrição |
| --- | --- | --- |
| `dim_cliente` | Dimensão | Cadastro único de clientes. |
| `dim_produto` | Dimensão | Detalhes e preços dos produtos. |
| `dim_pais` | Dimensão | Padronização de nomes geográficos. |
| `dim_tempo` | Dimensão | De-normalização de datas para análise temporal. |
| `fat_vendas` | Fato | Métricas de vendas ligadas às SKs das dimensões. |

## 🚀 Como fazer o Deploy

1. Clone o repositório.
2. Certifique-se de ter o [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html) configurado.
3. Execute o comando para deploy do bundle:
```bash
databricks bundle deploy

```

---

✍️ **Autor:** David Costa
