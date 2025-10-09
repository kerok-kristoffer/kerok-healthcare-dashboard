MERGE INTO gold_daily_staffing_fact t
USING (
  SELECT
    workdate, state, ccn,
    cast(hrs_rn AS decimal(18,2))  AS hrs_rn,
    cast(hrs_lpn AS decimal(18,2)) AS hrs_lpn,
    cast(hrs_cna AS decimal(18,2)) AS hrs_cna,
    cast(coalesce(hrs_rn,0)+coalesce(hrs_lpn,0)+coalesce(hrs_cna,0) AS decimal(18,2)) AS hrs_total_direct,
    cast(hrs_rn_emp AS decimal(18,2)) AS hrs_rn_emp,   cast(hrs_rn_ctr AS decimal(18,2)) AS hrs_rn_ctr,
    cast(hrs_lpn_emp AS decimal(18,2)) AS hrs_lpn_emp, cast(hrs_lpn_ctr AS decimal(18,2)) AS hrs_lpn_ctr,
    cast(hrs_cna_emp AS decimal(18,2)) AS hrs_cna_emp, cast(hrs_cna_ctr AS decimal(18,2)) AS hrs_cna_ctr,
    cast(mds_census_resident_count AS integer) AS residents
  FROM silver_pbj_daily
  -- optionally limit to source file if you add a source_file column
) s
ON (t.ccn = s.ccn AND t.workdate = s.workdate)
WHEN MATCHED THEN UPDATE SET
  state = s.state,
  hrs_rn = s.hrs_rn, hrs_lpn = s.hrs_lpn, hrs_cna = s.hrs_cna,
  hrs_total_direct = s.hrs_total_direct,
  hrs_rn_emp = s.hrs_rn_emp, hrs_rn_ctr = s.hrs_rn_ctr,
  hrs_lpn_emp = s.hrs_lpn_emp, hrs_lpn_ctr = s.hrs_lpn_ctr,
  hrs_cna_emp = s.hrs_cna_emp, hrs_cna_ctr = s.hrs_cna_ctr,
  residents = s.residents
WHEN NOT MATCHED THEN
INSERT VALUES (s.workdate, s.state, s.ccn, s.hrs_rn, s.hrs_lpn, s.hrs_cna, s.hrs_total_direct,
               s.hrs_rn_emp, s.hrs_rn_ctr, s.hrs_lpn_emp, s.hrs_lpn_ctr, s.hrs_cna_emp, s.hrs_cna_ctr, s.residents);
