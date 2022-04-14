-- Create Stock_Info Table

CREATE TABLE IF NOT EXISTS staging.S_PG_STOCK_INFO(
    Stock TEXT NOT NULL,
    Ticker TEXT NOT NULL PRIMARY KEY,
    Market TEXT NOT NULL,
    Industry TEXT NOT NULL,
    Summary TEXT NOT NULL
);