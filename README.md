# Projeto Delta Lakehouse com Databricks

Este projeto implementa uma arquitetura moderna de dados (Lakehouse) no Databricks na free edition, utilizando Delta Lake e Pyspark com as camadas **Bronze**, **Silver** e **Gold**. A orquestração é realizada por meio de notebooks organizados e versionados via GitHub.

---

## Estrutura das Camadas

- **Bronze**: Ingestão de dados brutos a partir de arquivos CSV.
- **Silver**: Limpeza, deduplicação e padronização de dados.
- **Gold**: Agregações e modelo analítico final (curated).

---

## Tecnologias Utilizadas

- [Databricks](https://databricks.com/) - Versao gratuida
- Delta Lake (formato de armazenamento)
- Apache Spark (Spark SQL e Pyspark)
- GitHub (versionamento de notebooks)
- Markdown (documentação)

---

## Estrutura de Pastas
```lua
├── databricks_notebook
│   ├── bronze.ipynb
│   ├── gold.ipynb
│   ├── settings.py
│   └── silver.ipynb
├── desenvolvimento_git.ipynb
├── docs
│   ├── catalog_lakehouse.JPG
│   ├── job_complete1.JPG
│   ├── job_complete2.JPG
│   └── job_complete3.JPG
├── raw_data
│   ├── customers.csv
│   ├── inventory_updates.csv
│   ├── order_items.csv
│   ├── orders.csv
│   └── products.csv
└── README.MD
```

---

## Execução com Databricks Jobs Pipelines
  - Este projeto utiliza os Jobs Pipelines do Databricks para orquestrar a execução dos notebooks que compõem cada camada da arquitetura Lakehouse (Bronze, Silver e Gold). Os notebooks são versionados em um repositório GitHub e conectados diretamente ao workspace do Databricks via a funcionalidade de Repos.
  - Cada notebook representa uma etapa do pipeline:
    - **bronze**.ipynb: Responsável pela ingestão dos dados brutos.
    - **silver**.ipynb: Realiza limpeza, filtragem e deduplicação dos dados.
    - **gold**.ipynb: Aplica agregações, enriquecimentos e transforma os dados para consumo analítico.

  - Os notebooks são executados em sequência por um Job configurado na interface do Databricks, garantindo automação e rastreabilidade dos processamentos.

## Configurações do Job
  - Tipo: Job Pipeline
  - Fonte: GitHub Repo conectado
  - Ambiente: Cluster compartilhado com Delta Lake ativado
  - Ordem de execução: Bronze → Silver → Gold

---

## Organização do Lakehouse e Catálogo**
Este projeto segue o padrão Lakehouse Architecture utilizando Delta Lake no Databricks, com dados organizados em três camadas: Raw, Bronze, Silver e Gold.

## Catálogo Personalizado
  - Todos os dados e tabelas Delta são salvos dentro de um catálogo personalizado chamado:
  - catalog: lakehouse
  - Dentro do catálogo lakehouse, os dados são organizados por esquemas (schemas) que representam as camadas:
    - raw – Armazena os arquivos brutos organizados por data de ingestão.
    - bronze – Tabelas Delta com dados normalizados e padronizados.
    - silver – Tabelas Delta limpas e transformadas.
    - gold – Tabelas Delta analíticas e agregadas.

  - Além de utilizar o armazenamento interno do Workspace do Databricks, uma boa opção também, o databricks permite configurar um Cloud Storage externo como fonte de dados para a camada Raw, como:
    - Amazon S3
    - Google Cloud Storage (GCS)
    - Azure Data Lake Storage (ADLS)
    Que por sinal tem bastante beneficios como:
      - Evita replicação desnecessária de dados.
      - Mantém os dados centralizados em uma fonte de verdade.
      - Permite que o Lakehouse seja consumido a partir de qualquer cloud provider, não limitado ao Databricks Workspace.


## Configurações do Spark por Camada

Cada camada da arquitetura (Bronze, Silver e Gold) utiliza uma SparkSession com configurações personalizadas para atender às necessidades de performance e otimização em cada etapa do pipeline:

**Bronze** Ingestão dos Dados Brutos
```bash
bronze_config = {
    "spark.sql.shuffle.partitions": "100",
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true",
    "spark.sql.parquet.filterPushdown": "true",
    "spark.sql.parquet.mergeSchema": "false",
    "spark.sql.files.ignoreCorruptFiles": "true",
    "spark.sql.files.ignoreMissingFiles": "true",
}
```
**Objetivo**: Evitar falhas na leitura, ignorar arquivos corrompidos e otimizar escrita e compactação automática.

---

**Silver** Limpeza e Padronização dos Dados
```bash
silver_config = {
    "spark.sql.shuffle.partitions": "200",
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true",
    "spark.sql.autoBroadcastJoinThreshold": "104857600",
    "spark.default.parallelism": "200",
    "spark.sql.parquet.filterPushdown": "true",
    "spark.databricks.delta.merge.enabled": "true",
}
```
**Objetivo**: Melhorar o desempenho de joins e atualizações com Delta.

**Benefícios**: Otimização de escrita, joins eficientes e uso de broadcast quando viável.

---

**Gold** (Camada Analítica)

```bash
gold_config = {
    "spark.sql.shuffle.partitions": "300",
    "spark.default.parallelism": "300",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true",
    "spark.sql.autoBroadcastJoinThreshold": "104857600",
    "spark.sql.parquet.filterPushdown": "true"
}
```
**Objetivo**: Ganho de performance com Adaptive Query Execution (AQE) em joins pesados.

**Benefícios**: Performance aprimorada em workloads analíticos com dados skewed (desequilibrados).

Essas configurações são aplicadas dinamicamente em cada notebook ao iniciar a SparkSession, garantindo que cada etapa esteja otimizada para sua função dentro da arquitetura Lakehouse.

