-- Total nurse hours by facility/state/month
CREATE OR REPLACE VIEW gold_vw_total_nurse_hours_monthly AS
SELECT
  date_trunc('month', workdate) AS month,
  state, ccn,
  SUM(COALESCE(hrs_rn,0)+COALESCE(hrs_lpn,0)+COALESCE(hrs_cna,0)) AS total_nurse_hours
FROM gold_daily_staffing_fact
GROUP BY 1,2,3;

-- Permanent vs Contract ratio by facility/state/month
CREATE OR REPLACE VIEW gold_vw_perm_vs_contract_monthly AS
SELECT
  date_trunc('month', workdate) AS month, state, ccn,
  SUM(COALESCE(hrs_rn_emp,0)+COALESCE(hrs_lpn_emp,0)+COALESCE(hrs_cna_emp,0)) AS perm_hours,
  SUM(COALESCE(hrs_rn_ctr,0)+COALESCE(hrs_lpn_ctr,0)+COALESCE(hrs_cna_ctr,0)) AS contract_hours,
  CAST(
    SUM(COALESCE(hrs_rn_emp,0)+COALESCE(hrs_lpn_emp,0)+COALESCE(hrs_cna_emp,0)) /
    NULLIF(CAST(SUM(COALESCE(hrs_rn_emp,0)+COALESCE(hrs_lpn_emp,0)+COALESCE(hrs_cna_emp,0)+
                  COALESCE(hrs_rn_ctr,0)+COALESCE(hrs_lpn_ctr,0)+COALESCE(hrs_cna_ctr,0)) AS DECIMAL(18,6)),0)
  AS DECIMAL(18,6)) AS perm_share
FROM gold_daily_staffing_fact
GROUP BY 1,2,3;
