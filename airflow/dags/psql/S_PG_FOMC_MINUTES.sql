
CREATE TABLE IF NOT EXISTS staging.S_PG_FOMC_MINUTES(
    Date_ TEXT NOT NULL,
    Contents TEXT NOT NULL,
    Speaker TEXT NOT NULL,
    Title TEXT NOT NULL,
    PRIMARY KEY (Date_, Contents, Speaker)
);
