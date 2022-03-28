CREATE OR REPLACE TABLE `{{ params.dwh_dataset }}.D_SG_IR` AS
SELECT
DISTINCT published_date AS DATE,
* EXCEPT (Date,
published_date,
preliminary,
timestamp,
interbank_overnight,
interbank_1w,
interbank_1m,
interbank_2m,
interbank_3m,
interbank_6m,
interbank_12m,
commercial_bills_3m,
usd_sibor_3m,
sgs_repo_overnight_rate),
FROM `{{ params.project_id }}.{{ params.staging_dataset }}.SG_IR_STAGING`;