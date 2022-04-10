CREATE OR REPLACE TABLE `{{ params.staging_destination_dataset }}.S_FEAR_GREED_INDEX` AS
SELECT
f.Date,
f.Access_Time as Access_Time,
f.FG_Value as FG_Index,
f.FG_Textvalue as FG_Value
FROM `{{ params.project_id }}.{{ params.staging_source_dataset }}.FEAR_GREED_INDEX_STAGING` f
INNER JOIN (
    SELECT Date, max(Access_Time) as MaxAccess_Time 
    FROM `{{ params.project_id }}.{{ params.staging_source_dataset }}.FEAR_GREED_INDEX_STAGING`
    GROUP BY Date
) fm on f.Date = fm.Date and f.Access_Time= fm.MaxAccess_Time
ORDER BY Date,
Access_Time DESC;