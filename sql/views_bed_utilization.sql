-- Bed utilization monthly by facility
CREATE OR REPLACE VIEW gold_vw_bed_utilization_facility_monthly AS
WITH m AS (
  SELECT
    date_trunc('month', f.workdate) AS month,
    f.state, f.ccn,
    COUNT(DISTINCT f.workdate) AS observed_days,
    SUM(CAST(COALESCE(f.residents,0) AS DECIMAL(18,4))) AS resident_days
  FROM gold_daily_staffing_fact f
  GROUP BY 1,2,3
),
b AS (
  SELECT DISTINCT ccn, state, certified_beds_reported
  FROM gold_quarterly_provider_fact
)
SELECT
  m.month, m.state, m.ccn,
  d.provider_name,
  m.observed_days, m.resident_days,
  b.certified_beds_reported,
  CAST(
    m.resident_days /
    NULLIF(CAST(b.certified_beds_reported AS DECIMAL(18,4)) * m.observed_days, 0)
  AS DECIMAL(18,4)) AS bed_utilization_rate_monthly
FROM m
LEFT JOIN b ON b.ccn = m.ccn AND b.state = m.state
LEFT JOIN gold_facility_dim d ON d.ccn = m.ccn;

-- Staffing vs occupancy (scatter-friendly)
CREATE OR REPLACE VIEW gold_vw_staffing_vs_occupancy AS
SELECT
  u.month, u.state, u.ccn, d.provider_name,
  u.bed_utilization_rate_monthly,
  h.hprd_weighted
FROM gold_vw_bed_utilization_facility_monthly u
LEFT JOIN (
  SELECT ccn, state,
         CAST(SUM(COALESCE(hrs_rn,0)+COALESCE(hrs_lpn,0)+COALESCE(hrs_cna,0)) AS DECIMAL(18,6)) /
         NULLIF(CAST(SUM(COALESCE(residents,0)) AS DECIMAL(18,6)),0) AS hprd_weighted
  FROM gold_daily_staffing_fact
  GROUP BY ccn, state
) h ON h.ccn = u.ccn AND h.state = u.state
LEFT JOIN gold_facility_dim d ON d.ccn = u.ccn;
