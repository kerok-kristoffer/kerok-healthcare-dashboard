MERGE INTO silver_pbj_daily t
USING (
  SELECT
    lpad(trim(PROVNUM),6,'0') AS ccn,
    trim(PROVNAME) AS provider_name,
    trim(CITY) AS city,
    trim(COUNTY_NAME) AS county,
    try_cast(COUNTY_FIPS AS integer) AS county_fips,
    upper(trim(STATE)) AS state,
    trim(CY_Qtr) AS cy_quarter,
    try_cast(WorkDate AS date) AS workdate,
    try_cast(MDScensus AS integer) AS mds_census_resident_count,
    try_cast(Hrs_RNDON AS decimal(9,2)) AS hrs_rndon,
    try_cast(Hrs_RNDON_emp AS decimal(9,2)) AS hrs_rndon_emp,
    try_cast(Hrs_RNDON_ctr AS decimal(9,2)) AS hrs_rndon_ctr,
    try_cast(Hrs_RNadmin AS decimal(9,2)) AS hrs_rnadmin,
    try_cast(Hrs_RNadmin_emp AS decimal(9,2)) AS hrs_rnadmin_emp,
    try_cast(Hrs_RNadmin_ctr AS decimal(9,2)) AS hrs_rnadmin_ctr,
    try_cast(Hrs_RN AS decimal(9,2)) AS hrs_rn,
    try_cast(Hrs_RN_emp AS decimal(9,2)) AS hrs_rn_emp,
    try_cast(Hrs_RN_ctr AS decimal(9,2)) AS hrs_rn_ctr,
    try_cast(Hrs_LPNadmin AS decimal(9,2)) AS hrs_lpnadmin,
    try_cast(Hrs_LPNadmin_emp AS decimal(9,2)) AS hrs_lpnadmin_emp,
    try_cast(Hrs_LPNadmin_ctr AS decimal(9,2)) AS hrs_lpnadmin_ctr,
    try_cast(Hrs_LPN AS decimal(9,2)) AS hrs_lpn,
    try_cast(Hrs_LPN_emp AS decimal(9,2)) AS hrs_lpn_emp,
    try_cast(Hrs_LPN_ctr AS decimal(9,2)) AS hrs_lpn_ctr,
    try_cast(Hrs_CNA AS decimal(9,2)) AS hrs_cna,
    try_cast(Hrs_CNA_emp AS decimal(9,2)) AS hrs_cna_emp,
    try_cast(Hrs_CNA_ctr AS decimal(9,2)) AS hrs_cna_ctr
  FROM bronze_pbj_daily_nurse_staffing_q2_2024_csv
  WHERE "$path" = :source_path
) s
ON (t.ccn = s.ccn AND t.workdate = s.workdate)
WHEN MATCHED THEN UPDATE SET
  provider_name = s.provider_name, city = s.city, county = s.county, county_fips = s.county_fips,
  state = s.state, cy_quarter = s.cy_quarter, mds_census_resident_count = s.mds_census_resident_count,
  hrs_rndon = s.hrs_rndon, hrs_rndon_emp = s.hrs_rndon_emp, hrs_rndon_ctr = s.hrs_rndon_ctr,
  hrs_rnadmin = s.hrs_rnadmin, hrs_rnadmin_emp = s.hrs_rnadmin_emp, hrs_rnadmin_ctr = s.hrs_rnadmin_ctr,
  hrs_rn = s.hrs_rn, hrs_rn_emp = s.hrs_rn_emp, hrs_rn_ctr = s.hrs_rn_ctr,
  hrs_lpnadmin = s.hrs_lpnadmin, hrs_lpnadmin_emp = s.hrs_lpnadmin_emp, hrs_lpnadmin_ctr = s.hrs_lpnadmin_ctr,
  hrs_lpn = s.hrs_lpn, hrs_lpn_emp = s.hrs_lpn_emp, hrs_lpn_ctr = s.hrs_lpn_ctr,
  hrs_cna = s.hrs_cna, hrs_cna_emp = s.hrs_cna_emp, hrs_cna_ctr = s.hrs_cna_ctr
WHEN NOT MATCHED THEN INSERT VALUES (
  s.ccn, s.provider_name, s.city, s.county, s.county_fips, s.state, s.cy_quarter, s.workdate,
  s.mds_census_resident_count, s.hrs_rndon, s.hrs_rndon_emp, s.hrs_rndon_ctr,
  s.hrs_rnadmin, s.hrs_rnadmin_emp, s.hrs_rnadmin_ctr,
  s.hrs_rn, s.hrs_rn_emp, s.hrs_rn_ctr, s.hrs_lpnadmin, s.hrs_lpnadmin_emp, s.hrs_lpnadmin_ctr,
  s.hrs_lpn, s.hrs_lpn_emp, s.hrs_lpn_ctr, s.hrs_cna, s.hrs_cna_emp, s.hrs_cna_ctr
);
