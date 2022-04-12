CREATE OR REPLACE TABLE `{{ params.staging_destination_dataset }}.S_FEAR_GREED_INDEX` AS
SELECT
f.Date,
f.FG_Value as FG_Index,
f.FG_Textvalue as FG_Value
FROM `{{ params.project_id }}.{{ params.staging_source_dataset }}.FEAR_GREED_INDEX_STAGING` f
ORDER BY Date