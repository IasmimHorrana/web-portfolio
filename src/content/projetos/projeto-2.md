---
title: Projeto PDF Pipeline
publishDate: 2026-04-30 00:00:00
description: Pipeline desenvolvida para automatizar o processamento de notas de corretagem em formato PDF.
tags:
  - Apache Airflow
  - PostgreSQL
  - PDF Extractor
  - Python
  - Data Engineering

github: https://github.com/IasmimHorrana/pdf-extract-pipeline

---

O **PDF Pipeline** é um pipeline de engenharia de dados desenvolvido para automatizar o processamento de notas de corretagem em formato PDF. O sistema monitora um bucket do MinIO, detecta novos arquivos, extrai informações estruturadas (texto e tabelas), parseia os dados relevantes e os armazena em um Data Warehouse no PostgreSQL.

O projeto foi concebido para rodar de forma contínua e idempotente, processando automaticamente novos PDFs assim que são enviados para o bucket de entrada. Cada nota de corretagem contém informações sobre operações de compra e venda de ativos, taxas cobradas e dados do cliente, que são transformado em dados estruturados para análise posterior.

A solução integra múltiplas tecnologias de código aberto, utilizando o Apache Airflow como orquestrador de tarefas, o MinIO como armazenamento de objetos compatível com S3, e o PostgreSQL como banco de dados relacional para persistência dos dados processados.

#### Stack Tecnológica

| Componente | Ferramenta | Versão | Descrição |
|------------|------------|--------|------------|
| Orquestração | Apache Airflow | 2.9.3 | Gerencia a execução das tarefas do pipeline |
| Armazenamento | MinIO | - | Armazenamento de objetos S3-compatible |
| Banco de Dados | PostgreSQL | - | Data Warehouse para dados estruturados |
| Extração de Texto | PyPDF2 | - | Leitura de conteúdo textual dos PDFs |
| Extração de Tabelas | Camelot | - | Detecção e extração de tabelas em PDFs |
| Bibliotecas Python | Pandas | 2.1.4 | Manipulação e análise de dados |
| Conexão S3 | boto3 | <1.35 | Cliente AWS S3 para MinIO |
| Logging | loguru | - | logging estruturado e simplicado |
| Validação | Pydantic | - | Validação de modelos de dados |
| Testes | pytest | - | Framework de testes unitários |
| Linting | ruff | - | Análise estática de código |

#### Etapa 1: Detecção de Novos Arquivos

O sensor MinIONewFileSensor verifica periodicamente (a cada 2 minutos) o bucket configurado na pasta uploads/. Quando novos arquivos são detectados, o sensor retorna a lista de objetos encontrados e trigger o próximo operador. O sensor utiliza o hook MinIOHook para conectar ao MinIO e listar os objetos disponíveis.

#### Etapa 2: Extração do PDF

O ExtractOperator baixa cada PDF do MinIO para o diretório temporário do Airflow. Em seguida, utiliza os extratores para capturar o conteúdo:

- O **TextExtractor** percorre todas as páginas do PDF e extrai o texto completo de cada uma, armazenando em uma lista de dicionários com o número da página e seu conteúdo.
- O **TableExtractor** tenta detectar tabelas usando primeiro o método lattice (para tabelas com bordas visíveis) e, se não encontrar, o método stream (para tabelas sem bordas claras).
- O **PDFExtractor** combina ambos os extratores em um único resultado estruturado.

#### Etapa 3: Parse dos Dados

Após a extração, o operador parseia o conteúdo textual usando expressões regulares para identificar campos como número da nota, data de pregão, cliente, corretora, conta de liquidação, operações e taxas. O parser foi desenvolvido para reconhecer códigos de ações com final 3 (como VALE3, BBAS3) e final 4 (como PETR4, BBDC4).

#### Etapa 4: Load no PostgreSQL

O LoadOperator utiliza o NotasCorretagemLoader para inserir ou atualizar os dados no banco. O loader realiza um UPSERT (INSERT ou UPDATE) para garantir idempotência, evitando duplicatas em caso de retry do Airflow. Os dados são salvos em três tabelas do schema dw (notas_corretagem, operacoes, taxas) e um backup em JSON no schema staging.

#### Etapa 5: Movimentação do PDF

Após a extração bem-sucedida, o operador move o PDF da pasta `uploads/` para `processed/`, garantindo que o mesmo arquivo não seja processado novamente em execuções futuras.

#### Estrutura de Pastas

A estrutura de pastas do projeto segue convenções claras para organização do código, configurações e recursos. Cada diretório tem uma finalidade específica que facilita a manutenção e a evolução do sistema.

```
projeto-pdf-pipeline/
├── config/                          # Configurações do ambiente
│   ├── docker-compose.yaml          # Compose principal (Airflow + pgAdmin)
│   └── .env                         # Variáveis de ambiente
│
├── dags/                            # DAGs e componentes Airflow
│   ├── pdf_pipeline_dag.py          # DAG principal do pipeline
│   ├── hooks/                       # Hooks de conexão
│   │   ├── __init__.py
│   │   ├── minio_hook.py           # Hook MinIO/S3
│   │   └── postgres_hook.py        # Hook PostgreSQL
│   ├── operators/                   # Operators customizados
│   │   ├── __init__.py
│   │   ├── extract_operator.py     # Extrai dados do PDF
│   │   └── load_operator.py        # Carrega no PostgreSQL
│   └── sensors/                    # Sensors customizados
│       ├── __init__.py
│       └── minio_sensor.py         # Detecta novos PDFs
│
├── docker/                          # Dockerfiles customizados
│   └── Dockerfile                  # Airflow com dependências
│
├── docs/                            # Documentação adicional
│
├── logs/                            # Logs da aplicação
│
├── samples/                         # PDFs de exemplo para teste
│   ├── Notas_Corretagem_Final-1.pdf
│   └── Notas_Corretagem_Final-2.pdf
│
├── sql/                            # Scripts SQL (schemas)
│   └── notas_corretagem.sql       # Schema Data Warehouse
│
├── scripts/                        # Scripts de desenvolvimento
│   ├── pdf_viz.py                  # Visualização de PDF
│   └── preview_notas_loader.py     # Preview sem insert
│
├── src/                            # Código fonte principal
│   ├── __init__.py
│   ├── extractors/                 # Extratores de PDF
│   │   ├── __init__.py
│   │   ├── text_extractor.py      # Extração de texto (PyPDF2)
│   │   ├── table_extractor.py     # Extração de tabelas (Camelot)
│   │   └── pdf_extractor.py       # Wrapper combinando ambos
│   │
│   ├── loaders/                    # Loaders (PostgreSQL)
│   │   ├── __init__.py
│   │   └── notas_corretagem_loader.py  # Loader principal
│   │
│   ├── models/                     # Modelos de dados (Pydantic)
│   │
│   ├── utils/                      # Utilitários
│   │   ├── __init__.py
│   │   └── logging_config.py      # Configuração de logging
│   │
│   └── validators/                 # Validadores de dados
│
├── tests/                          # Testes unitários
│   ├── __init__.py
│   ├── test_text_extractor.py
│   ├── test_table_extractor.py
│   ├── test_pdf_extractor.py
│   └── test_notas_loader.py
│
├── pyproject.toml                  # Configurações do projeto (UV)
├── uv.lock                         # Lock de dependências
├── README.md                       # Documentação básica
├── DOCUMENTATION.md                # Documentação completa
├── SESSOES_GUIA.md                 # Histórico de desenvolvimento
└── MELHORIAS.md                    # Lista de melhorias futuras
```

A organização segue o princípio de separação de responsabilidades, onde código de negócio (src/), configurações de orquestração (dags/), testes (tests/) e recursos estáticos (samples/, sql/) mantêm suas próprias estruturas independentes.

#### Hooks

Os hooks abstraem a conexão com sistemas externos, permitindo que os operadores interajam com o MinIO e PostgreSQL de forma padronizada.

**MinIOHook** (`dags/hooks/minio_hook.py`): Fornece métodos para conectar ao MinIO usando boto3, listar objetos, baixar arquivos, carregar arquivos e mover objetos entre pastas. Utiliza a connection configurada no Airflow para obter credenciais e endpoint.

**PostgresHook** (`dags/hooks/postgres_hook.py`): Abstração para conexão PostgreSQL via Airflow. Métodos principais incluem get_conn() para obter conexão, get_records() para SELECT, run() para INSERT/UPDATE/DELETE, run_many() para operações em batch, e close() para fechar a conexão.

#### Sensors

**MinIONewFileSensor** (`dags/sensors/minio_sensor.py`): Sensor que verifica continuamente a pasta uploads/ do bucket MinIO em busca de novos arquivos. Implementa o padrão poke do Airflow, verificando periodicamente (poke_interval) até encontrar arquivos novos ou atingir o timeout configurado.

#### Operators

**ExtractOperator** (`dags/operators/extract_operator.py`): Operator personalizado que baixa PDFs do MinIO, extrai texto e tabelas usando os extratores do src/, parseia os dados e salva o resultado em JSON temporário. Após a extração bem-sucedida, move o PDF para a pasta processed/ no MinIO, garantindo idempotência.

**LoadOperator** (`dags/operators/load_operator.py`): Operator que lê o JSON temporário gerado pelo ExtractOperator e utiliza o NotasCorretagemLoader para inserir ou atualizar os dados no PostgreSQL. Salva os dados estruturados nas tabelas dw e mantém um backup em staging.

#### DAG Principal

**pdf_pipeline_dag.py**: Define o workflow completo do pipeline. Configurado para rodar a cada 2 minutos (schedule_interval='*/2 * * * *'), com as tarefas detect_new_files, extract_pdf e load_pdf_data connected em sequência.

#### Extractors e Loaders

Os extratores e loaders são o núcleo do processamento de dados, transformando PDFs não estruturados em dados tabulares organizados.

#### TextExtractor

O TextExtractor utiliza a biblioteca PyPDF2 para ler o conteúdo textual de cada página do PDF. Para cada página, cria um dicionário contendo o número da página e o texto extraído. O método extract() retorna uma lista de dicionários, um para cada página do documento.

A extração de texto é útil para capturar informações como dados do cliente, número da nota, data de pregão e outras informações que aparecem no corpo do documento, mas que não estão estruturadas em formato de tabela.

#### TableExtractor

O TableExtractor utiliza a biblioteca Camelot para detectar e extrair tabelas dos PDFs. O extrator tenta primeiro o método lattice (que funciona bem para tabelas com bordas visíveis e linhas definidas) e, se não encontrar resultados, tenta o método stream (que detecta tabelas por análise de espaçamento e alinhamento).

O método extract() retorna uma lista de dicionários, onde cada entrada contém o número da página e os dados da tabela em formato de lista de listas (cada sublista representa uma linha da tabela).

#### PDFExtractor

O PDFExtractor é um wrapper que combina os resultados do TextExtractor e TableExtractor em um único objeto estruturado. Fornece uma interface unificada para extrair todo o conteúdo relevante de um PDF, simplificando o trabalho dos operadores que o utilizam.

#### NotasCorretagemLoader

O NotasCorretagemLoader é responsável por transformar os dados extraídos e parseados em INSERTs e UPDATEs no PostgreSQL. Implementa idempotência através de uma constraint UNIQUE na tabela dw.notas_corretagem, garantindo que a mesma nota não seja inserida duas vezes.

O loader divide os dados em três partes: dados principais da nota (cliente, corretora, conta, etc.), operações (compra/venda de ativos) e taxas (IRRF, corretagem, registro). Cada parte é inserida em sua respective tabela, com as devidas relações através de foreign keys.

#### Schema do Banco de Dados

O banco de dados utiliza dois schemas distintos: dw (Data Warehouse) para dados de negócio e staging para dados brutos de auditoria.

#### Schema dw (Data Warehouse)

O schema dw contém as tabelas com dados estruturados e validados do negócio.

**dw.notas_corretagem**: Armazena as informações principais de cada nota de corretagem.

| Coluna | Tipo | Descrição |
|--------|------|------------|
| id | SERIAL | Chave primária |
| file_name | VARCHAR(255) | Nome do arquivo PDF processado |
| cliente | VARCHAR(255) | Nome do cliente |
| corretora | VARCHAR(255) | Nome da corretora |
| conta_liquidacao | VARCHAR(50) | Conta de liquidação |
| numero_fatura | INTEGER | Número da nota |
| data_pregao | DATE | Data do pregão (YYYY-MM-DD) |
| upload_date | TIMESTAMP | Timestamp do processamento |
| processed_date | TIMESTAMP | Data em que foi processado |
| created_at | TIMESTAMP | Data de criação do registro |

Constraints: UNIQUE(corretora, conta_liquidacao, numero_fatura, data_pregao)

**dw.operacoes**: Armazena as operações de compra e venda de cada nota.

| Coluna | Tipo | Descrição |
|--------|------|------------|
| id | SERIAL | Chave primária |
| nota_id | INTEGER | FK para dw.notas_corretagem |
| operacao | VARCHAR(10) | Compra (C) ou Venda (V) |
| mercadoria | VARCHAR(20) | Código do ativo (PETR4, VALE3, etc.) |
| quantidade | INTEGER | Quantidade de contratos |
| cotacao | NUMERIC(12,2) | Preço por contrato |
| tipo_mercado | VARCHAR(20) | Tipo de mercado |
| created_at | TIMESTAMP | Data de criação |

**dw.taxas**: Armazena as taxas cobradas em cada nota.

| Coluna | Tipo | Descrição |
|--------|------|------------|
| id | SERIAL | Chave primária |
| nota_id | INTEGER | FK para dw.notas_corretagem |
| irrf | NUMERIC(12,2) | Imposto de Renda Retido na Fonte |
| ajuste | NUMERIC(12,2) | Ajuste de custo |
| taxa_corretagem | NUMERIC(12,2) | Taxa de corretagem |
| taxa_registro | NUMERIC(12,2) | Taxa de registro |
| created_at | TIMESTAMP | Data de criação |