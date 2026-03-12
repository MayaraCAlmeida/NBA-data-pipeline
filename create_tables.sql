---- Games
CREATE TABLE IF NOT EXISTS games (
    team_id       BIGINT,
    team_abbr     VARCHAR(10),
    team_name     VARCHAR(100),
    game_id       VARCHAR(20),
    game_date     DATE,
    matchup       VARCHAR(20),
    win_loss      VARCHAR(1),
    points        NUMERIC(6,2),
    point_diff    NUMERIC(6,2),
    is_home       SMALLINT,
    is_win        SMALLINT,
    PRIMARY KEY (team_id, game_id)
);

---- Player Game Logs 
CREATE TABLE IF NOT EXISTS player_gamelogs (
    player_id           BIGINT,
    player_name         VARCHAR(100),
    team_id             BIGINT,
    team_abbr           VARCHAR(10),
    game_id             VARCHAR(20),
    game_date           DATE,
    matchup             VARCHAR(20),
    win_loss            VARCHAR(1),
    is_win              SMALLINT,
    is_home             SMALLINT,
    minutes             NUMERIC(6,2),
    fg_made             NUMERIC(5,1),
    fg_attempts         NUMERIC(5,1),
    fg_pct              NUMERIC(5,3),
    fg3_made            NUMERIC(5,1),
    fg3_attempts        NUMERIC(5,1),
    fg3_pct             NUMERIC(5,3),
    ft_made             NUMERIC(5,1),
    ft_attempts         NUMERIC(5,1),
    ft_pct              NUMERIC(5,3),
    off_rebounds        NUMERIC(5,1),
    def_rebounds        NUMERIC(5,1),
    rebounds            NUMERIC(5,1),
    assists             NUMERIC(5,1),
    turnovers           NUMERIC(5,1),
    steals              NUMERIC(5,1),
    blocks              NUMERIC(5,1),
    blocked_att         NUMERIC(5,1),
    fouls               NUMERIC(5,1),
    fouls_drawn         NUMERIC(5,1),
    points              NUMERIC(5,1),
    plus_minus          NUMERIC(6,1),
    -- Métricas avançadas
    true_shooting_pct   NUMERIC(6,4),
    ast_to_tov_ratio    NUMERIC(6,3),
    impact_score        NUMERIC(8,4),
    game_score          NUMERIC(8,4),
    usage_proxy         NUMERIC(6,2),
    -- Outliers
    points_zscore       NUMERIC(8,4),
    points_outlier      BOOLEAN,
    impact_score_zscore NUMERIC(8,4),
    impact_score_outlier BOOLEAN,
    -- Rolling 5 jogos
    points_rolling5     NUMERIC(8,4),
    assists_rolling5    NUMERIC(8,4),
    rebounds_rolling5   NUMERIC(8,4),
    impact_score_rolling5 NUMERIC(8,4),
    -- Rolling 10 jogos
    points_rolling10    NUMERIC(8,4),
    assists_rolling10   NUMERIC(8,4),
    rebounds_rolling10  NUMERIC(8,4),
    impact_score_rolling10 NUMERIC(8,4),
    PRIMARY KEY (player_id, game_id)
);

--- Player Season Stats 
CREATE TABLE IF NOT EXISTS player_season_stats (
    player_id           BIGINT PRIMARY KEY,
    player_name         VARCHAR(100),
    team_abbr           VARCHAR(10),
    games_played        INTEGER,
    wins                INTEGER,
    win_rate            NUMERIC(5,4),
    avg_minutes         NUMERIC(6,2),
    avg_points          NUMERIC(6,2),
    avg_assists         NUMERIC(6,2),
    avg_rebounds        NUMERIC(6,2),
    avg_steals          NUMERIC(6,2),
    avg_blocks          NUMERIC(6,2),
    avg_turnovers       NUMERIC(6,2),
    avg_plus_minus      NUMERIC(6,2),
    avg_ts_pct          NUMERIC(6,4),
    avg_ast_tov         NUMERIC(6,3),
    avg_impact_score    NUMERIC(8,4),
    avg_game_score      NUMERIC(8,4),
    max_points          NUMERIC(5,1),
    max_impact          NUMERIC(8,4),
    std_points          NUMERIC(8,4),
    consistency_score   NUMERIC(8,4),
    impact_rank         INTEGER
);

--- Índices para performance 
CREATE INDEX IF NOT EXISTS idx_gamelogs_player   ON player_gamelogs (player_id);
CREATE INDEX IF NOT EXISTS idx_gamelogs_date     ON player_gamelogs (game_date);
CREATE INDEX IF NOT EXISTS idx_gamelogs_team     ON player_gamelogs (team_id);
CREATE INDEX IF NOT EXISTS idx_games_date        ON games (game_date);
CREATE INDEX IF NOT EXISTS idx_season_rank       ON player_season_stats (impact_rank);
