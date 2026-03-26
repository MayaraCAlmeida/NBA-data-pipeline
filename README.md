# NBA pipeline

Pipeline ETL que coleta estatísticas da NBA direto da API oficial, calcula métricas avançadas e gera um dashboard interativo. Roda automaticamente todo dia — sem precisar fazer nada manualmente.

---

## Fluxo

```
extract_data.py       →  API da NBA  →  dados_brutos/
clean_data.py         →  dados_brutos/  →  dados_processados/
transform_data.py     →  métricas, outliers, rolling  →  dados_processados/
load_database.py      →  dados_processados/  →  PostgreSQL
generate_dashboard.py →  PostgreSQL  →  nba_dashboard.html
```

---

## Arquivos

| Arquivo | O que faz |
|---|---|
| `extract_data.py` | Busca game logs, líderes (PTS/AST/REB/STL/BLK), resultados e metadata. Retry automático, respeita o rate limit da API |
| `clean_data.py` | Renomeia colunas pra snake_case, converte tipos, remove jogos sem minutos, cria `is_win` e `is_home`, deduplica |
| `transform_data.py` | Calcula Impact Score, True Shooting %, Game Score, usage proxy, Z-score por jogador, rolling de 5 e 10 jogos |
| `load_database.py` | Upsert no PostgreSQL — insere novos registros e atualiza os existentes. Valida FKs antes de subir |
| `create_tables.sql` | Cria as 5 tabelas e a view `vw_team_performance`. Idempotente (`IF NOT EXISTS`) |
| `analytics_queries.sql` | 10 queries prontas pra usar no banco ou em qualquer ferramenta de BI |
| `generate_dashboard.py` | Conecta no banco, puxa os dados reais e regera o `nba_dashboard.html` |
| `nba_dashboard.html` | Dashboard interativo com 4 abas — abre direto no navegador |
| `scheduler.py` | Orquestra o pipeline local. Suporta execução imediata (`--run-now`) ou agendamento diário |
| `daily_pipeline.yml` | GitHub Actions — mesma coisa, mas na nuvem, sem precisar do PC ligado |
| `.env.example` | Template das variáveis do banco |

---

## Métricas calculadas

**Impact Score** — resume o impacto do jogador num número só:
```
(PTS×0.35) + (AST×0.20) + (REB×0.20) + (STL×0.12) + (BLK×0.08) − (TOV×0.15)
```

**True Shooting %** — eficiência real de arremesso, contando lances livres e três pontos:
```
PTS / (2 × (FGA + 0.44 × FTA))
```

**Game Score** (Hollinger) — nota geral do jogo:
```
PTS + 0.4×FGM − 0.7×FGA − 0.4×(FTA−FTM) + 0.7×OREB + 0.3×DREB + STL + 0.7×AST + 0.7×BLK − 0.4×PF − TOV
```

Além disso: Z-score por jogador pra detectar jogos fora da curva (threshold > 2.5) e rolling averages dos últimos 5 e 10 jogos para pontos, assistências, rebotes e impact score.

---

## Banco de dados

5 tabelas + 1 view:

- `teams` — metadata dos times
- `players` — metadata dos jogadores ativos
- `games` — resultado por time/jogo
- `player_gamelogs` — box score completo + métricas derivadas por jogo
- `player_season_stats` — agregados da temporada por jogador
- `league_leaders` — snapshot dos líderes por categoria
- `vw_team_performance` — view com win rate, pontuação média e saldo por time

---

## Dashboard

Gerado pelo `generate_dashboard.py` com dados reais do banco. Tem 4 abas:

- **Overview** — KPIs gerais, top 50 por Impact Score, tier dos jogadores (Elite / High Impact / Consistent / Developing)
- **Players** — perfil individual com radar chart e tendência de forma
- **Teams** — classificação por win rate, pontuação e saldo de pontos
- **Trends** — evolução mensal e distribuição de Impact Score

---

## Queries prontas

`analytics_queries.sql` tem 10 queries pra rodar direto:

1. Top 20 por Impact Score (mín. 10 jogos)
2. Evolução de pontos — rolling 5 e 10 dos top 10
3. Home vs away por time
4. Jogos fora da curva — Z-score > 2.5
5. True Shooting Top 30 (mín. 15 jogos)
6. Ranking de times na temporada
7. Quadrante consistência vs produção (Elite / High Impact / Consistent / Developing)
8. Head-to-head entre dois jogadores — troca os nomes antes de rodar
9. Melhores jogos individuais da temporada por Game Score
10. Tendência mensal dos top 5

---

## Configuração

```bash
pip install apscheduler nba_api pandas numpy scipy sqlalchemy psycopg2-binary python-dotenv
```

```bash
cp .env.example .env
```

`.env`:
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=nba_pipeline
DB_USER=postgres
DB_PASSWORD=sua_senha
```

> O `.env` já está no `.gitignore`.

Antes de rodar pela primeira vez, cria as tabelas no banco:
```bash
psql -U postgres -d nba_pipeline -f create_tables.sql
```

---

## Rodando

```bash
# pipeline completo
python scheduler.py --run-now

# só o dashboard (banco já tem dados)
python generate_dashboard.py
```

---

## Agendamento

**Local** — deixa rodando e ele dispara todo dia às 06:00 ET:
```bash
python scheduler.py
```

Pra mudar o horário:
```bash
python scheduler.py --hour 7 --minute 30
```

**Na nuvem** — o `daily_pipeline.yml` faz a mesma coisa via GitHub Actions. Coloca as credenciais do banco em `Settings → Secrets → Actions`:

| Secret | Valor |
|---|---|
| `DB_HOST` | host do banco em nuvem |
| `DB_PORT` | 5432 |
| `DB_NAME` | nba_pipeline |
| `DB_USER` | seu usuário |
| `DB_PASSWORD` | sua senha |

---

## Stack

| | |
|---|---|
| Python 3.11 | linguagem principal |
| nba_api | fonte de dados |
| pandas + numpy | transformação |
| scipy | Z-score / outliers |
| PostgreSQL 16 | banco de dados |
| SQLAlchemy | conexão e upsert |
| APScheduler | agendamento local |
| GitHub Actions | automação na nuvem |
