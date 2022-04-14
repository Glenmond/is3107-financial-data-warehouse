CREATE TABLE IF NOT EXISTS staging.S_PG_FOMC_STATEMENT(
    Date_ TEXT NOT NULL,
    contents TEXT NOT NULL,
    speaker TEXT NOT NULL,
    title TEXT NOT NULL,
    PRIMARY KEY (Date_, contents, speaker)
);
