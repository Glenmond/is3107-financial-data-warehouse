-- Create Stock_Fundamentals Table

CREATE TABLE IF NOT EXISTS staging.S_PG_STOCK_FUNDAMENTALS(
    Date_ TEXT NOT NULL,
    Total_Revenue decimal NULL,
    Total_Assets decimal NULL,
    Cash decimal NULL,
    Total_Current_Liabilities decimal NULL,
    Total_Liab decimal NULL,
    Net_Income decimal NULL,
    Total_Stockholder_Equity decimal NULL,
    Common_Stock decimal NULL,
    Treasury_Stock decimal NULL,
    Ticker TEXT NOT NULL,
    Stock TEXT NOT NULL,
    PRIMARY KEY (Date_, Ticker)
);
