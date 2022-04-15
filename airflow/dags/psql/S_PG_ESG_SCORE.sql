CREATE TABLE IF NOT EXISTS staging.S_PG_ESG_SCORE(
    Date_ TEXT NOT NULL,
    Symbol TEXT NULL,
    Sector TEXT NULL,
    previousClose DECIMAL NULL,
    sharesOutstanding DECIMAL NULL,
    socialScore DECIMAL NULL,
    governanceScore DECIMAL NULL,
    totalEsg DECIMAL NULL,
    environmentScore DECIMAL NULL,
    PRIMARY KEY (Date_)
);

