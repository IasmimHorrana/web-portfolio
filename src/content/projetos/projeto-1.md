---
title: Weather ETL Pipeline — Salvador, Bahia
publishDate: 2026-05-01 00:00:00
tags:
  - Data Engineering
  - Python
  - Airflow
  - Docker
  - Postgres
  - MinIO
  - Metabase
github: https://github.com/IasmimHorrana/weather-api
---

### Por que este projeto existe?

Salvador é uma cidade tropical com um regime de chuvas intenso e imprevisível. Eventos de chuva forte e vendavais causam alagamentos, acidentes e impactam diretamente a vida da população. A maioria das pessoas depende de apps de previsão do tempo que mostram "vai chover" — mas não dizem **o quanto**, **com qual intensidade** e, principalmente, **se há risco real de emergência**.

Este projeto vai além de um simples termômetro digital. Ele foi construído para ser um **sistema de monitoramento ativo de risco ambiental**: coleta dados climáticos reais a cada hora, aplica critérios técnicos do Instituto Nacional de Meteorologia (INMET) para classificar o nível de risco e, quando necessário, **envia um alerta automático via Telegram**, sem que ninguém precise olhar para um painel.

O projeto foi também uma oportunidade de aplicar na prática as principais disciplinas de **Engenharia de Dados** — desde a extração automatizada de uma API pública até a construção de um Data Warehouse consultável por um dashboard de Business Intelligence.

---

### O que o sistema faz, em linguagem simples

1. **A cada hora**, o sistema acorda automaticamente e pergunta para uma API meteorológica: "Como está o clima em Salvador agora?"
2. O sistema **salva esse dado bruto** com segurança em um repositório de arquivos, sem modificar nada.
3. Em seguida, ele **processa e enriquece** esses dados: converte unidades, classifica condição climática e calcula se o momento representa risco NORMAL, ATENÇÃO, ALERTA ou CRÍTICO.
4. Esse dado tratado é **persistido em um banco de dados relacional**, formando um histórico.
5. **Views analíticas** são aplicadas sobre o banco, deixando os dados prontos para consulta por um dashboard.
6. Se o risco for ALERTA ou CRÍTICO, uma **notificação é enviada automaticamente via Telegram**.
7. Todo esse ciclo é **monitorado visualmente** pelo Apache Airflow, que registra cada execução, sucesso ou falha.

---

### Arquitetura: A Lógica por Trás da Estrutura

O projeto segue a **Arquitetura Medallion**, um padrão amplamente utilizado na indústria de dados que organiza as informações em três camadas — Bronze, Silver e Gold — cada uma com responsabilidade específica.

```
  ┌───────────────────────────────────────────────┐
  │          Apache Airflow  (@hourly)            │
  │   Scheduler ──────────── Webserver            │
  └───────────────────┬───────────────────────────┘
                      │  orquestra 5 tarefas
                      ▼
  Open-Meteo API (gratuita, sem chave de acesso)
        │
        ▼
  [ extract.py ]  → Requisição HTTP + validação + retry
        │
        ├──▶  MinIO — Camada Bronze  (JSON bruto, imutável)
        │         weather_data/YYYY-MM-DD/HH-MM-SS_salvador.json
        │
        ▼
  [ transform.py ] → Limpeza, tipagem, matriz de risco INMET
        │
        ├──▶  MinIO — Camada Silver  (dados enriquecidos)
        │
        ├──▶  [ load.py ]      → PostgreSQL (tb_weather_history)
        │
        ├──▶  [ gold.py ]      → Atualiza views analíticas no PostgreSQL
        │
        └──▶  [ alertas.py ]   → Verifica risco → Telegram (se ALERTA/CRÍTICO)
```

### Por que separar em Bronze, Silver e Gold?

Imagine que um dado chega errado da API — um campo a menos, uma unidade diferente. Se você salvar apenas o dado processado, **perdeu o dado original para sempre**. A arquitetura Medallion protege contra isso:

| Camada | Onde fica | O que contém | Por que existe |
|--------|-----------|--------------|----------------|
| **Bronze** | MinIO (bucket **bronze**) | JSON bruto da API, exatamente como chegou | Histórico imutável. Permite reprocessar qualquer dado passado se a lógica mudar |
| **Silver** | MinIO (bucket **silver**) | Dados tratados, tipados e com nível de risco calculado | Fonte confiável para carga no banco. Elimina ruído e garante qualidade |
| **Gold** | PostgreSQL (views) | Agregações e séries temporais prontas para análise | Desempenho otimizado para dashboards. Consulta rápida sem transformar dados na hora |

---

### A Infraestrutura: Docker Compose

Todo o ambiente é encapsulado em contêineres Docker, o que significa que qualquer pessoa pode subir o projeto completo com um único comando, sem instalar dependências manualmente na máquina.

```bash
docker compose -f infra/docker-compose.yml up -d --build
```

O `docker-compose.yml` orquestra **6 serviços** que conversam entre si por uma rede interna:

| Serviço | Porta | Função |
|---------|-------|--------|
| **MinIO** | 9000 / 9001 | Object Storage S3-compatível. Armazena os arquivos JSON das camadas Bronze e Silver. Interface web em **localhost:9001** |
| **PostgreSQL** | 5432 | Banco de dados relacional. Armazena o histórico de leituras (**weather_db**) e os metadados internos do Airflow (**airflow_db**) |
| **pgAdmin** | 5050 | Interface gráfica para explorar o banco PostgreSQL diretamente |
| **Airflow Webserver** | 8080 | Interface visual do Airflow. Mostra o grafo de tarefas, histórico de execuções e logs |
| **Airflow Scheduler** | — | Processo em background que lê os DAGs e dispara as tarefas no horário programado |
| **Metabase** | 3000 | Plataforma de Business Intelligence conectada ao PostgreSQL para criação de dashboards |

> ##### O MinIO é compatível com a API S3 da Amazon. Isso significa que para migrar para a nuvem real (AWS S3 ou Google Cloud Storage), basta mudar uma variável de ambiente. Além disso, o storage em object store é muito mais robusto, escalável e não depende do sistema de arquivos do servidor.

---

### Etapa 1 — Extração: 

**O que faz:** Realiza a requisição HTTP para a [Open-Meteo API](https://open-meteo.com/), valida a resposta e persiste o JSON bruto no MinIO (Bronze).

**Por que a Open-Meteo?** É uma API meteorológica de código aberto, gratuita, sem necessidade de criar conta ou chave de acesso. Fornece dados de previsão e tempo atual com alta precisão, incluindo temperatura, umidade, chuva, velocidade do vento e código de condição climática (padrão WMO).

```
URL usada:
api.open-meteo.com/v1/forecast
  ?latitude=-12.9711          ← coordenadas de Salvador, BA
  &longitude=-38.5108
  &current=temperature_2m, relative_humidity_2m, rain, weather_code, wind_speed_10m
  &timezone=America/Bahia
```

**Decisões técnicas:**
- A requisição tem **timeout=10s** para evitar que o pipeline trave se a API demorar.
- Em caso de erro HTTP ou falha de rede, a função loga o erro e retorna vazio, sem derrubar o Airflow.
- O arquivo salvo no MinIO tem path no formato **weather_data/YYYY-MM-DD/HH-MM-SS_salvador.json**, permitindo particionamento natural por data.

---

### Etapa 2 — Transformação: 

**O que faz:** Lê o JSON bruto da camada Bronze, aplica uma sequência de transformações e persiste o resultado tratado na camada Silver. A transformação é uma esteira com **6 etapas encadeadas**:

### 2.1 Achatamento 
A Open-Meteo retorna um JSON com campos aninhados (dentro de **"current"**). Essa função extrai todos os campos relevantes e os organiza em uma tabela (DataFrame), que é a estrutura ideal para processar dados tabulares.

### 2.2 Conversão de Datas 
A API retorna o horário como um número Unix Timestamp (segundos desde 01/01/1970 em UTC). Essa função converte para o fuso horário **America/Bahia**, garantindo que o horário registrado seja sempre o horário local de Salvador.

### 2.3 Tradução do Código WMO 
A API retorna a condição climática como um número (ex: **61**). Uma tabela de mapeamento converte esse número para texto legível (ex: **"Chuva leve"**), seguindo o padrão internacional WMO (World Meteorological Organization).

### 2.4 Validação de Chuva 
Em dias sem chuva, a API pode simplesmente omitir o campo **rain**. Essa função garante que a coluna sempre exista e valha **0.0** quando ausente, evitando que dados nulos quebrem etapas posteriores.

### 2.5 Cálculo do Nível de Risco 
Essa é a **regra de negócio central** do projeto. Baseada nos parâmetros oficiais do INMET, classifica cada leitura em 4 níveis:

| Nível | Condição |
|-------|----------|
| **CRÍTICO** | Chuva ≥ 50mm/h **OU** vento ≥ 20m/s **OU** (chuva ≥ 30mm/h **E** vento ≥ 15m/s) |
| **ALERTA**  | Chuva ≥ 30mm/h |
| **ATENÇÃO** | Chuva ≥ 10mm/h **E** umidade ≥ 90% |
| **NORMAL**  | Nenhuma das condições acima |

O cálculo usa **numpy.select**, uma operação vetorial que processa todas as linhas ao mesmo tempo — muito mais eficiente que um loop condicional.

### 2.6 Exportação para Silver 
O DataFrame enriquecido é serializado de volta para JSON e enviado ao MinIO no bucket **silver**, com o mesmo path da Bronze substituindo o prefixo.

---

### Etapa 3 — Carga: 

**O que faz:** Lê o JSON processado da camada Silver e insere as linhas na tabela **tb_weather_history** do PostgreSQL.

**Idempotência** — A palavra que evita dados duplicados:
O pipeline roda a cada hora. Se por algum motivo ele executar duas vezes para o mesmo snapshot, **o banco de dados não vai duplicar o registro**. Isso é garantido pela combinação de:

1. **Constraint única no banco:** UNIQUE (cidade, data_hora) — o banco rejeita dois registros para a mesma cidade no mesmo instante.
2. **INSERT ... ON CONFLICT DO NOTHING** no código — a instrução SQL tenta inserir, e se já existir, ignora silenciosamente sem gerar erro.

O insert usa **SQLAlchemy** e é executado dentro de uma transação: ou tudo é inserido, ou nada — garantindo consistência.

---

### Etapa 4 — Camada Gold: 

**O que faz:** Aplica (ou recria) as views analíticas no PostgreSQL, deixando os dados prontos para o Metabase.

As views são arquivos **.sql** dentro de **infra/postgres/gold/** e são aplicadas automaticamente pelo **gold.py** a cada execução do pipeline. Isso garante que mudanças na lógica de análise sejam propagadas automaticamente.

**5 views disponíveis:**

| View | Descrição |
| :--- | :--- |
| **vw_gold_condicao_atual**        | Leitura mais recente para consulta rápida |
| **vw_gold_resumo_diario**         | Agregações diárias (temp, chuva, vento) |
| **vw_gold_estatisticas_semanais** | Médias e totais por semana e riscos |
| **vw_gold_tendencia_temperatura** | Série temporal com média móvel de 3h |
| **vw_gold_alertas_historico**     | Registros CRÍTICO ou ALERTA |

---

### Etapa 5 — Alertas: 

**O que faz:** Verifica se alguma leitura do ciclo atual classificou como CRÍTICO ou ALERTA e, em caso positivo, envia uma notificação formatada via Telegram.

**Por que Telegram?** É gratuito, tem API simples e confiável, funciona em qualquer celular e permite mensagens formatadas com Markdown. A notificação chega como uma mensagem de bot com todos os dados relevantes:

```
🚨 ALERTA METEOROLÓGICO — CRÍTICO

Cidade: Salvador (BR)
Chuva (1h): 55.0 mm
Vento: 18.0 m/s
Umidade: 97%
Temperatura: 26.0°C
Data/Hora: 2026-05-07T15:00:00-03:00
```

**Resiliência por design:**
- A função usa **Tenacity** para fazer **3 tentativas** com intervalo de 2 segundos caso a API do Telegram falhe temporariamente.
- Se após as 3 tentativas ainda falhar, o pipeline **não quebra** — o erro é logado e a execução continua normalmente.
- As credenciais (Bot Token e Chat ID) são lidas de variáveis de ambiente, nunca hardcoded no código.

---

### Camada de Storage: 

**O que faz:** É a abstração completa de toda comunicação com o MinIO (leitura e escrita nas camadas Bronze e Silver).

**Decisão de design importante:** Todos os outros módulos (**extract.py, transform.py, load.py, alertas.py**) dependem de **storage.py** para qualquer operação no MinIO. Isso cria um **ponto único de troca**: para migrar de MinIO para AWS S3 real em produção, basta atualizar as variáveis de ambiente — o restante do código não muda nada.

O cliente boto3 é criado com **functools.lru_cache**, garantindo que seja um Singleton: uma única conexão é compartilhada por toda a execução, evitando overhead de reconexão a cada chamada.

---

### Orquestração: Apache Airflow + DAG

**O que é um DAG?** DAG significa *Directed Acyclic Graph* (Grafo Acíclico Dirigido). No Airflow, ele é o mapa de execução do pipeline: define quais tarefas existem, em que ordem rodam e quais dependem de quais.

O DAG **coleta_salvador** (**dags/dag_coleta_salvador.py**) é agendado para rodar **a cada hora** (**@hourly**) com **catchup=False** — ou seja, se o sistema ficar offline por 3 horas e voltar, ele **não tenta recuperar** as 3 execuções perdidas. Apenas a próxima execução programada é disparada.

**Fluxo de execução com dois ramos paralelos:**

```
extract_bronze
      │
transform_silver
      │          │
load_historico   dispara_alertas   ← ramos independentes
      │
apply_gold_views
```

A carga no banco e os alertas rodam em **ramos separados e independentes após a transformação**. Isso significa que se o Telegram cair, a carga no banco continua — e vice-versa.

**XCom — Comunicação entre tarefas:**
As tarefas trocam dados entre si via XCom (sistema interno do Airflow). A tarefa **extract_bronze** empurra a **bronze_key** (o path do arquivo no MinIO), e a **transform_silver** puxa essa chave para saber qual arquivo processar — sem depender de variáveis globais.

---

### Testes Unitários: 

O projeto tem **6 arquivos de teste** cobrindo todos os módulos críticos. A suíte usa **Pytest** como framework e **unittest.mock** para simular dependências externas (MinIO, Telegram, banco de dados).

**Por que mockar as dependências?**
Testes unitários devem ser rápidos, previsíveis e independentes de infraestrutura. Usar **patch** substitui a função real (ex: **_enviar_telegram**) por uma versão falsa que simula o comportamento esperado — sem precisar de internet, banco de dados ou credenciais reais.

### Exemplos de cenários testados:

**test_transform.py** (9 classes, 35+ testes):
- Achatamento do JSON retorna exatamente 1 linha
- Conversão de km/h para m/s no campo de vento
- Chuva ausente na API → coluna preenchida com 0.0
- Código WMO **61** → "Chuva leve"
- Chuva ≥ 50mm → nível CRÍTICO
- CRÍTICO vence ALERTA quando ambos são verdadeiros
- Dados originais nunca são modificados (imutabilidade)

**test_alertas.py** (2 classes, 9 testes):
- Situação NORMAL → nenhum alerta disparado (retorno = 0)
- Situação CRÍTICA → alerta disparado (retorno = 1)
- DataFrame sem coluna **nivel_risco**` → pipeline não quebra
- API do Telegram fora do ar → pipeline não quebra
- Múltiplas cidades → apenas as críticas geram notificação

---

### CI/CD: GitHub Actions

**O que é CI?** Continuous Integration (Integração Contínua) é a prática de validar automaticamente o código toda vez que uma mudança é enviada ao repositório. Se algo quebrar, o sistema avisa antes que chegue em produção.

O workflow **.github/workflows/ci.yml** é acionado em todo **push** ou **pull_request** para a branch **main** e executa **5 verificações em sequência**:

```
1. Checkout Code           → Clona o repositório
2. Setup Python 3.12       → Prepara o ambiente
3. Install uv              → Gerenciador de dependências ultrarrápido
4. Install Dependencies    → uv sync --all-extras --dev

5. ✅ Ruff Format Check    → Verifica se o código está formatado corretamente
6. ✅ Ruff Lint            → Verifica se há código morto, imports não usados, etc.
7. ✅ MyPy Type Check      → Verifica se os tipos estão corretos (ex: string ≠ número)
8. ✅ Pytest + Coverage    → Roda todos os testes. Falha se cobertura < 70%
9. ✅ pip-audit            → Verifica se alguma dependência tem vulnerabilidade CVE
```

**Por que **uv**` e não **pip**?** O **uv** é um gerenciador de pacotes escrito em Rust, até **10x mais rápido** que o **pip** para instalar dependências. Com cache habilitado no CI, o tempo de setup cai drasticamente.

**Por que verificar vulnerabilidades (pip-audit)?** Um projeto de dados frequentemente usa dezenas de bibliotecas. Se uma delas tiver uma vulnerabilidade de segurança conhecida (CVE), **pip-audit** detecta e o CI falha, forçando a atualização antes que o problema chegue em produção.

---

### Qualidade de Código

| Ferramenta | Papel |
|------------|-------|
| **Ruff** | Linter + formatter ultrarrápido. Aplica PEP-8, ordena imports, remove código morto |
| **MyPy** | Verifica tipos estáticos. **disallow_untyped_defs = true** força que toda função tenha tipos declarados |
| **Pytest** | Framework de testes. **--cov-fail-under=70** rejeita o CI se menos de 70% do código estiver coberto por testes |
| **pip-audit** | Auditoria de segurança contra vulnerabilidades CVE nas dependências |

---

### Dashboard: Metabase

O Metabase é conectado diretamente ao PostgreSQL e consome as views da camada Gold. Em **localhost:3000**, é possível criar dashboards com:

- Gráfico de temperatura ao longo do tempo (com média móvel de 3h)
- Histórico de chuvas diárias e semanais
- Contagem de eventos por nível de risco (NORMAL, ATENÇÃO, ALERTA, CRÍTICO)
- Tabela com todos os momentos de CRÍTICO e ALERTA registrados

Não é necessário saber SQL para explorar os dados: o Metabase tem interface visual de arrastar e soltar para criar consultas e gráficos.

---

### Stack Tecnológica Completa

### Linguagem e Gerenciamento
- **Python 3.12** — Linguagem principal
- **uv** — Gerenciador de dependências e ambientes virtuais

### Bibliotecas Python
| Biblioteca | Versão | Uso |
|------------|--------|-----|
| **pandas** | ≥3.0 | Manipulação de dados tabulares |
| **numpy** | — | Operações vetoriais (cálculo de risco) |
| **boto3** | ≥1.42 | Client S3 para comunicação com MinIO |
| **sqlalchemy** | ≥2.0 | ORM e engine de conexão com PostgreSQL |
| **psycopg2-binary** | ≥2.9 | Driver PostgreSQL para Python |
| **requests** | ≥2.33 | Requisições HTTP para a API |
| **tenacity** | ≥9.1 | Retries automáticos com backoff |
| **python-dotenv** | ≥1.2 | Carregamento de variáveis de ambiente |

### Qualidade e Testes
| Ferramenta | Versão | Uso |
|------------|--------|-----|
| **pytest**     | ≥9.0 | Framework de testes |
| **pytest-cov** | ≥7.1 | Relatório de cobertura de testes |
| **ruff**       | ≥0.15 | Linter e formatter |
| **mypy**       | ≥1.20 | Verificação de tipos estáticos |
| **pip-audit**  | ≥2.10 | Auditoria de segurança |

### Infraestrutura
| Tecnologia | Versão | Uso |
|------------|--------|-----|
| Docker | — | Containerização de todos os serviços |
| Docker Compose | — | Orquestração local dos contêineres |
| MinIO | latest | Object Storage S3-compatível |
| PostgreSQL | 15 Alpine | Banco de dados relacional |
| Apache Airflow | 2.10+ | Orquestração e agendamento do pipeline |
| Metabase | latest | Dashboard de Business Intelligence |
| GitHub Actions | — | CI/CD automatizado |

---

### Estrutura de Pastas

```
weather-api/
├── .github/
│   └── workflows/
│       └── ci.yml             ← Pipeline de CI/CD
├── config/
│   └── .env                   ← Variáveis de ambiente (credenciais)
├── dags/
│   └── dag_coleta_salvador.py ← DAG do Airflow com as 5 tarefas
├── infra/
│   ├── airflow/
│   │   └── Dockerfile         ← Imagem customizada do Airflow com as libs do projeto
│   ├── docker-compose.yml     ← Definição de todos os serviços
│   └── postgres/
│       ├── init.sql            ← Criação da tabela e índices
│       ├── 00_init_airflow.sh  ← Script para criar o banco interno do Airflow
│       └── gold/
│           ├── vw_alertas_historico.sql
│           ├── vw_condicao_atual.sql
│           ├── vw_estatisticas_semanais.sql
│           ├── vw_resumo_diario.sql
│           └── vw_tendencia_temperatura.sql
├── src/
│   ├── extract.py             ← Extração da API para Bronze
│   ├── transform.py           ← Transformação Bronze → Silver
│   ├── load.py                ← Carga Silver → PostgreSQL
│   ├── gold.py                ← Aplicação das views analíticas
│   ├── alertas.py             ← Notificações via Telegram
│   └── storage.py             ← Abstração de acesso ao MinIO
├── tests/
│   ├── test_extract.py
│   ├── test_transform.py
│   ├── test_load.py
│   ├── test_gold.py
│   ├── test_storage.py
│   └── test_alertas.py
└── pyproject.toml             ← Configurações do projeto, dependências e ferramentas
```

---

### Como Executar o Projeto

### Pré-requisitos
- Docker Desktop instalado
- Python 3.12+
- **uv** instalado (**pip install uv**)

### Passo a passo

```bash
# 1. Clone o repositório
git clone https://github.com/IasmimHorrana/Weather-ETL-Pipeline.git
cd Weather-ETL-Pipeline

# 2. Configure as variáveis de ambiente
cp config/.env.example config/.env
# Edite o .env com seu TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID

# 3. Suba toda a infraestrutura (build + start)
docker compose -f infra/docker-compose.yml up -d --build
# Aguarde ~60s para o airflow-init terminar

# 4. Instale as dependências locais (para desenvolvimento e testes)
uv sync --all-extras --dev
```

### Acessando os serviços

| Serviço | URL | Credenciais |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| Metabase | http://localhost:3000 | Configure na primeira abertura |
| MinIO | http://localhost:9001 | minioadmin / minioadmin123 |
| pgAdmin | http://localhost:5050 | admin@admin.com / admin |

### Comandos de desenvolvimento

```bash
# Rodar todos os testes com relatório de cobertura
uv run pytest -v --cov=src --cov-report=term-missing

# Verificar e corrigir formatação e lint
uv run ruff format src/ tests/ dags/
uv run ruff check --fix src/ tests/ dags/

# Verificar tipos estáticos
uv run mypy src/ dags/

# Auditoria de segurança
uv run pip-audit
```

---

### Resultados e Evolução

O sistema acumula histórico contínuo desde o primeiro dia em produção. Após semanas rodando, o banco de dados possui centenas de leituras horárias, todas classificadas por nível de risco, disponíveis para análise temporal no Metabase.
