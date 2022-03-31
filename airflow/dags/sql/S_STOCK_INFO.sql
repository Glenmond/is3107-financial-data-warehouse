CREATE OR REPLACE TABLE `{{ params.staging_destination_dataset }}.S_STOCK_INFO` AS
SELECT
Stock as Stock,
Ticker as Stock_id,
Industry as Stock_industry,
Summary as Stock_summary
FROM `{{ params.project_id }}.{{ params.staging_source_dataset }}.STOCK_INFO_STAGING`;