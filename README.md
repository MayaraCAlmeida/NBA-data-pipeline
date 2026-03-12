# NBA Performance Monitor — Data Pipeline

> Pipeline automatizado de dados esportivos que coleta, transforma, armazena e analisa estatísticas da NBA em tempo real.

---

## Visão Geral

Pipeline ETL completo para dados da NBA — do dado bruto até o dashboard, rodando automaticamente todo dia.

---

## Fluxo Completo

```
extract_data.py
      │
      ▼
 dados_brutos/
      │
      ▼
clean_data.py
      │
      ▼
 dados_processados/
      │
      ▼
transform_data.py
      │
      ▼
create_tables.sql
      │
      ▼
load_database.py
      │
      ▼
analytics_queries.sql
      │
      ▼
generate_dashboard.py
      │
      ▼
nba_dashboard.html
```

---

## Arquivos

| Arquivo | O que faz |
|---|---|
| `extract_data.py` | Coleta dados da API oficial da NBA |
| `clean_data.py` | Padroniza colunas, trata nulos, remove duplicatas |
| `transform_data.py` | Calcula métricas avançadas, outliers e rolling averages |
| `create_tables.sql` | Cria as tabelas e índices no PostgreSQL |
| `load_database.py` | Faz upsert dos dados no banco |
| `analytics_queries.sql` | 10 queries analíticas prontas |
| `generate_dashboard.py` | Gera o dashboard HTML com os dados reais do banco |
| `nba_dashboard.html` | Dashboard interativo com gráficos |
| `scheduler.py` | Orquestra e agenda o pipeline completo |
| `daily_pipeline.yml` | GitHub Actions — automação na nuvem |
| `.env.example` | Template de configuração do banco |

---

## 1. Extração — `extract_data.py`

Coleta dados direto da API oficial da NBA via `nba_api`:

- Game logs de todos os jogadores da temporada 2024-25
- Líderes de estatísticas (PTS, AST, REB, STL, BLK)
- Resultados de jogos
- Metadata de jogadores e times

Salva tudo em `dados_brutos/` com timestamp no nome do arquivo. Inclui retry automático e respeita o rate limit da API.

---

## 2. Limpeza — `clean_data.py`

Lê os arquivos brutos e aplica:

- Renomeação de colunas para snake_case legível
- Conversão de tipos (datas, numéricos)
- Remoção de jogos sem minutos jogados
- Criação das flags `is_win` e `is_home`
- Eliminação de duplicatas por chave primária

Salva os dados limpos em `dados_processados/`.

---

## 3. Transformação — `transform_data.py`

Feature engineering com métricas avançadas:

**Por jogo:**

```
Impact Score = (PTS×0.35) + (AST×0.20) + (REB×0.20) + (STL×0.12) + (BLK×0.08) − (TOV×0.15)

True Shooting % = PTS / (2 × (FGA + 0.44 × FTA))

Game Score (Hollinger) = PTS + 0.4×FGM − 0.7×FGA − 0.4×(FTA−FTM)
                        + 0.7×OREB + 0.3×DREB + STL + 0.7×AST + 0.7×BLK − 0.4×PF − TOV
```

**Detecção de outliers** via Z-score por jogador (threshold > 2.5)

**Rolling averages** dos últimos 5 e 10 jogos para pontos, assistências, rebotes e impact score

**Agregados da temporada** por jogador: médias, máximos, win rate, consistency score e impact rank

---

## 4. Banco de Dados — `create_tables.sql` + `load_database.py`

`create_tables.sql` cria as tabelas de forma idempotente (`IF NOT EXISTS`):

- `player_gamelogs` — todos os jogos com métricas derivadas
- `player_season_stats` — agregados da temporada por jogador
- `games` — resultados por time
- Índices para performance nas consultas

`load_database.py` carrega os dados com upsert inteligente — insere novos registros e atualiza os existentes, sem duplicatas mesmo rodando várias vezes.

---

## 5. Análises — `analytics_queries.sql`

10 queries prontas para uso direto no banco ou em ferramentas de BI:

1. Top 20 jogadores por Impact Score
2. Evolução de forma (rolling 5 jogos)
3. Desempenho home vs away por time
4. Jogos outlier — performances fora da curva
5. Eficiência ofensiva — True Shooting Top 30
6. Ranking de times na temporada
7. Consistência vs produção (quadrante Elite / High Impact / Consistent / Developing)
8. Comparação head-to-head entre dois jogadores
9. Melhores jogos individuais da temporada
10. Tendência mensal de pontos

---

## 6. Dashboard — `generate_dashboard.py` + `nba_dashboard.html`

`generate_dashboard.py` conecta ao PostgreSQL, puxa os dados reais e gera o `nba_dashboard.html` atualizado.

O dashboard tem 4 abas:

- **Overview** — KPIs gerais, ranking por Impact Score, outliers
- **Players** — perfil individual com radar chart e tendência de forma
- **Teams** — classificação e comparativo home vs away
- **Trends** — evolução mensal e distribuição de Impact Score

---

## 7. Agendamento — `scheduler.py`

Orquestra o pipeline completo (extração → limpeza → transformação → carga) e pode rodar de duas formas:

**Execução imediata (manual):**

```bash
cd C:\Users\Usuário\Downloads\NBA
python scheduler.py --run-now
```

**Agendamento diário automático:**

```bash
python scheduler.py
```

O scheduler serve para rodar o pipeline automaticamente todo dia, sem você precisar fazer nada. Como a NBA joga toda noite, os dados mudam diariamente. O scheduler garante que às 06:00 ET — quando todos os jogos da noite já terminaram — ele sozinho vai:

- Buscar os dados novos da API
- Limpar
- Calcular as métricas
- Atualizar o banco

---

## 8. Automação na Nuvem — `daily_pipeline.yml`

Este é o GitHub Actions — a versão profissional do scheduler, que roda na nuvem sem precisar do seu PC ligado.

O que ele faz:

- Roda automaticamente todo dia às 09:00 UTC (06:00 ET) via cron
- Ou manualmente pelo botão na interface do GitHub
- Sobe uma máquina Ubuntu, instala as dependências, executa o pipeline completo
- Salva o log de cada execução por 7 dias
- Abre uma issue automaticamente no repositório se o pipeline falhar, te avisando do problema

Nos Secrets do repositório (`Settings → Secrets → Actions`) você coloca o host do banco em nuvem no lugar de `localhost` e funciona perfeitamente:

| Secret | Valor |
|---|---|
| `DB_HOST` | host do banco em nuvem |
| `DB_PORT` | 5432 |
| `DB_NAME` | nba_pipeline |
| `DB_USER` | seu usuário |
| `DB_PASSWORD` | sua senha |

---

## Configuração — `.env.example`

Crie um arquivo `.env` na pasta do projeto baseado no template:

```bash
cp .env.example .env
```

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=nba_pipeline
DB_USER=postgres
DB_PASSWORD=sua_senha
```

> ⚠️ O `.env` com suas credenciais reais **nunca deve ser commitado**. Ele já está no `.gitignore`.

---

## Como Rodar

```bash
# 1. Instalar dependências
pip install apscheduler nba_api pandas numpy scipy sqlalchemy psycopg2-binary python-dotenv

# 2. Configurar o banco
cp .env.example .env
# editar .env com suas credenciais

# 3. Rodar o pipeline completo
python scheduler.py --run-now

# 4. Gerar o dashboard (opcional — abre no navegador)
python generate_dashboard.py
```

---

## Stack

| Tecnologia | Uso |
|---|---|
| Python 3.11 | Orquestração |
| nba_api | Fonte de dados |
| pandas + numpy | Transformação |
| scipy | Detecção de outliers |
| PostgreSQL 16 | Banco de dados |
| SQLAlchemy | Conexão e upsert |
| APScheduler | Agendamento local |
| GitHub Actions | Automação na nuvem |

---