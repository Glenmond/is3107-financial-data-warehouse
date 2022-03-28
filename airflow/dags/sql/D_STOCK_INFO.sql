INSERT `{{ params.dwh_dataset }}.D_STOCK_INFO`
SELECT
Stock as Stock,
Ticker as Stock_id,
Industry as Stock_industry,
Summary as Stock_summary
FROM `{{ params.project_id }}.{{ params.staging_dataset }}.STOCK_INFO_STAGING`;