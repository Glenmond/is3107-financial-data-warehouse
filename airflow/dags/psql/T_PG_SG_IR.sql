DROP TABLE IF EXISTS T_PG_SG_IR;

CREATE TABLE IF NOT EXISTS T_PG_SG_IR AS
SELECT 
DISTINCT CAST(published_date as Date) AS Date_,
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
FROM staging.S_PG_SG_IR
ORDER BY Date_ DESC;