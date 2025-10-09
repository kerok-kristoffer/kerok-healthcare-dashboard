MERGE INTO gold_quarterly_provider_fact t
USING (
  SELECT
    ccn, state,
    DATE '2024-04-01' AS reporting_period_start,
    DATE '2024-06-30' AS reporting_period_end,
    '2024Q2' AS reporting_period_quarter,
    cast(average_number_of_residents_per_day AS decimal(10,2)) AS residents_per_day_reported,
    cast(adjusted_total_nurse_staffing_hours_per_resident_per_day AS decimal(10,4)) AS total_nurse_hprd_adj_reported,
    cast(reported_total_nurse_staffing_hours_per_resident_per_day AS decimal(10,4)) AS total_nurse_hprd_reported,
    cast(number_of_certified_beds AS integer) AS certified_beds_reported,
    cast(number_of_fines AS integer) AS fines_count_reported,
    cast(total_amount_of_fines_in_dollars AS decimal(18,2)) AS fines_usd_reported,
    cast(number_of_payment_denials AS integer) AS payment_denials_reported,
    cast(total_number_of_penalties AS integer) AS penalties_reported,
    current_timestamp AS snapshot_received_ts
  FROM silver_providerinfo
) s
ON (t.ccn = s.ccn AND t.reporting_period_quarter = s.reporting_period_quarter)
WHEN MATCHED THEN UPDATE SET
  state = s.state,
  residents_per_day_reported = s.residents_per_day_reported,
  total_nurse_hprd_adj_reported = s.total_nurse_hprd_adj_reported,
  total_nurse_hprd_reported = s.total_nurse_hprd_reported,
  certified_beds_reported = s.certified_beds_reported,
  fines_count_reported = s.fines_count_reported,
  fines_usd_reported = s.fines_usd_reported,
  payment_denials_reported = s.payment_denials_reported,
  penalties_reported = s.penalties_reported,
  snapshot_received_ts = s.snapshot_received_ts
WHEN NOT MATCHED THEN INSERT VALUES (
  s.ccn, s.state, s.reporting_period_start, s.reporting_period_end, s.reporting_period_quarter,
  s.residents_per_day_reported, s.total_nurse_hprd_adj_reported, s.total_nurse_hprd_reported,
  s.certified_beds_reported, s.fines_count_reported, s.fines_usd_reported,
  s.payment_denials_reported, s.penalties_reported, s.snapshot_received_ts
);
