CREATE OR REPLACE TABLE `{{ params.staging_destination_dataset }}.S_SG_IR` AS
SELECT 
DISTINCT CAST(published_date as Date) AS Date,
aggregate_volume, 
calculation_method,
comp_sora_1m,
comp_sora_3m,
comp_sora_6m,
sor_average,
sora,
sora_index,
standing_facility_borrow,
standing_facility_deposit
FROM `{{ params.project_id }}.{{ params.staging_source_dataset }}.SG_IR_STAGING`
ORDER BY Date DESC;