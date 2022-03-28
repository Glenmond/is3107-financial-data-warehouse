IF
  (
  SELECT
    COUNT(1)
  FROM
    `{{ params.dwh_dataset }}.D_EXCHANGE_RATE`) = 0 
THEN
    CREATE OR REPLACE TABLE `{{ params.dwh_dataset }}.D_EXCHANGE_RATE` AS
    SELECT
    Date,
    Ticker as Exchange_rate_id,
    Commodities as Name_id,
    High,
    Low,
    Open,
    Close,
    Adj_Close,
    Volume
    FROM `{{ params.project_id }}.{{ params.staging_dataset }}.EXCHANGE_RATE_STAGING`
    ORDER BY
    Date,
    Exchange_rate_id;
ELSE
    INSERT `{{ params.dwh_dataset }}.D_EXCHANGE_RATE`
    SELECT
    Date,
    Ticker as Exchange_rate_id,
    Commodities as Name_id,
    High,
    Low,
    Open,
    Close,
    Adj_Close,
    Volume
    FROM `{{ params.project_id }}.{{ params.staging_dataset }}.EXCHANGE_RATE_STAGING`
    ORDER BY
    Date,
    Exchange_rate_id;
END IF;