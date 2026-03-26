--  NBA — Schema completo

--  Dimensões

CREATE TABLE IF NOT EXISTS teams (
    team_id      BIGINT PRIMARY KEY,
    full_name    VARCHAR(100) NOT NULL,
    abbreviation VARCHAR(10)  NOT NULL,
    nickname     VARCHAR(50),
    city         VARCHAR(100),
    state        VARCHAR(100),
    year_founded SMALLINT
);

CREATE TABLE IF NOT EXISTS players (
    player_id  BIGINT PRIMARY KEY,
    full_name  VARCHAR(100) NOT NULL,
    first_name VARCHAR(50),
    last_name  VARCHAR(50),
    is_active  BOOLEAN DEFAULT TRUE
);



--  Jogos (resultado por time)

CREATE TABLE IF NOT EXISTS games (
    team_id    BIGINT        NOT NULL,
    team_abbr  VARCHAR(10),
    team_name  VARCHAR(100),
    game_id    VARCHAR(20)   NOT NULL,
    game_date  DATE          NOT NULL,
    matchup    VARCHAR(30),
    win_loss   VARCHAR(1),
    points     NUMERIC(6,2),
    point_diff NUMERIC(6,2),
    is_home    SMALLINT,
    is_win     SMALLINT,
    PRIMARY KEY (team_id, game_id),
    FOREIGN KEY (team_id) REFERENCES teams (team_id)
);

CREATE INDEX IF NOT EXISTS idx_games_date ON games (game_date);
CREATE INDEX IF NOT EXISTS idx_games_team ON games (team_id);



--  Game logs por jogador


CREATE TABLE IF NOT EXISTS player_gamelogs (
    player_id    BIGINT      NOT NULL,
    player_name  VARCHAR(100),
    team_id      BIGINT,
    team_abbr    VARCHAR(10),
    game_id      VARCHAR(20) NOT NULL,
    game_date    DATE        NOT NULL,
    matchup      VARCHAR(30),
    win_loss     VARCHAR(1),
    is_win       SMALLINT,
    is_home      SMALLINT,
    -- Box score
    minutes      NUMERIC(6,2),
    fg_made      NUMERIC(5,1),
    fg_attempts  NUMERIC(5,1),
    fg_pct       NUMERIC(5,3),
    fg3_made     NUMERIC(5,1),
    fg3_attempts NUMERIC(5,1),
    fg3_pct      NUMERIC(5,3),
    ft_made      NUMERIC(5,1),
    ft_attempts  NUMERIC(5,1),
    ft_pct       NUMERIC(5,3),
    off_rebounds NUMERIC(5,1),
    def_rebounds NUMERIC(5,1),
    rebounds     NUMERIC(5,1),
    assists      NUMERIC(5,1),
    turnovers    NUMERIC(5,1),
    steals       NUMERIC(5,1),
    blocks       NUMERIC(5,1),
    blocked_att  NUMERIC(5,1),
    fouls        NUMERIC(5,1),
    fouls_drawn  NUMERIC(5,1),
    points       NUMERIC(5,1),
    plus_minus   NUMERIC(6,1),
    -- Métricas derivadas
    -- true_shooting_pct  = pts / (2 * (fga + 0.44 * fta))
    -- ast_to_tov_ratio   = assists / NULLIF(turnovers, 0)
    -- game_score         = pts + 0.4*fg - 0.7*fga - 0.4*(fta-ft) + 0.7*oreb + 0.3*dreb + stl + 0.7*ast + 0.7*blk - 0.4*pf - tov
    -- impact_score       = game_score + plus_minus
    -- usage_proxy        = (fga + 0.44*fta + tov) / NULLIF(minutes, 0)
    true_shooting_pct  NUMERIC(6,4),
    ast_to_tov_ratio   NUMERIC(6,3),
    game_score         NUMERIC(8,4),
    impact_score       NUMERIC(8,4),
    usage_proxy        NUMERIC(6,4),
    -- Outliers (z-score calculado dentro da temporada)
    points_zscore        NUMERIC(8,4),
    points_outlier       BOOLEAN,
    impact_score_zscore  NUMERIC(8,4),
    impact_score_outlier BOOLEAN,
    -- Rolling 5 jogos (ordenado por game_date por jogador)
    points_rolling5       NUMERIC(8,4),
    assists_rolling5      NUMERIC(8,4),
    rebounds_rolling5     NUMERIC(8,4),
    impact_score_rolling5 NUMERIC(8,4),
    -- Rolling 10 jogos
    points_rolling10       NUMERIC(8,4),
    assists_rolling10      NUMERIC(8,4),
    rebounds_rolling10     NUMERIC(8,4),
    impact_score_rolling10 NUMERIC(8,4),
    PRIMARY KEY (player_id, game_id),
    FOREIGN KEY (player_id) REFERENCES players (player_id),
    FOREIGN KEY (team_id)   REFERENCES teams (team_id)
);

CREATE INDEX IF NOT EXISTS idx_gamelogs_player ON player_gamelogs (player_id);
CREATE INDEX IF NOT EXISTS idx_gamelogs_date   ON player_gamelogs (game_date);
CREATE INDEX IF NOT EXISTS idx_gamelogs_team   ON player_gamelogs (team_id);



--  Estatísticas agregadas por temporada

CREATE TABLE IF NOT EXISTS player_season_stats (
    player_id        BIGINT      NOT NULL,
    season_year      VARCHAR(10) NOT NULL,   -- ex: '2024-25'
    player_name      VARCHAR(100),
    team_abbr        VARCHAR(10),
    games_played     INTEGER,
    wins             INTEGER,
    win_rate         NUMERIC(5,4),
    avg_minutes      NUMERIC(6,2),
    avg_points       NUMERIC(6,2),
    avg_assists      NUMERIC(6,2),
    avg_rebounds     NUMERIC(6,2),
    avg_steals       NUMERIC(6,2),
    avg_blocks       NUMERIC(6,2),
    avg_turnovers    NUMERIC(6,2),
    avg_plus_minus   NUMERIC(6,2),
    avg_ts_pct       NUMERIC(6,4),
    avg_ast_tov      NUMERIC(6,3),
    avg_impact_score NUMERIC(8,4),
    avg_game_score   NUMERIC(8,4),
    max_points       NUMERIC(5,1),
    max_impact       NUMERIC(8,4),
    std_points       NUMERIC(8,4),
    -- consistency_score = 1 / (1 + std_points)  →  quanto menor o desvio, maior o score
    consistency_score NUMERIC(8,4),
    impact_rank       INTEGER,
    PRIMARY KEY (player_id, season_year),
    FOREIGN KEY (player_id) REFERENCES players (player_id)
);

CREATE INDEX IF NOT EXISTS idx_season_rank ON player_season_stats (season_year, impact_rank);



--  Líderes de estatísticas (snapshot por categoria)

CREATE TABLE IF NOT EXISTS league_leaders (
    player_id        BIGINT      NOT NULL,
    season_year      VARCHAR(10) NOT NULL,
    stat_category    VARCHAR(50) NOT NULL,   -- ex: 'PTS', 'AST', 'REB'
    rank             INTEGER,
    player_name      VARCHAR(100),
    team_abbr        VARCHAR(10),
    games_played     INTEGER,
    minutes_per_game NUMERIC(8,2),
    fg_made          NUMERIC(8,2),
    fg_attempts      NUMERIC(8,2),
    fg_pct           NUMERIC(5,3),
    fg3_made         NUMERIC(8,2),
    fg3_attempts     NUMERIC(8,2),
    fg3_pct          NUMERIC(5,3),
    ft_made          NUMERIC(8,2),
    ft_attempts      NUMERIC(8,2),
    ft_pct           NUMERIC(5,3),
    off_rebounds     NUMERIC(8,2),
    def_rebounds     NUMERIC(8,2),
    rebounds         NUMERIC(8,2),
    assists          NUMERIC(8,2),
    turnovers        NUMERIC(8,2),
    steals           NUMERIC(8,2),
    blocks           NUMERIC(8,2),
    points_per_game  NUMERIC(8,2),
    efficiency       NUMERIC(8,2),
    PRIMARY KEY (player_id, season_year, stat_category),
    FOREIGN KEY (player_id) REFERENCES players (player_id)
);

CREATE INDEX IF NOT EXISTS idx_leaders_category ON league_leaders (season_year, stat_category, rank);




-----------------------------------------

SELECT schemaname, tablename 
FROM pg_tables 
WHERE tablename = 'games';

DROP TABLE IF EXISTS league_leaders;
DROP TABLE IF EXISTS player_season_stats;
DROP TABLE IF EXISTS player_gamelogs;
DROP TABLE IF EXISTS games CASCADE;
DROP TABLE IF EXISTS players;
DROP TABLE IF EXISTS teams;
