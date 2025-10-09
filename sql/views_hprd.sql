MERGE INTO gold_facility_dim t
USING (
  SELECT
    ccn,
    trim(provider_name) AS provider_name,
    upper(trim(state))  AS state,
    trim(city)          AS city,
    trim(county)        AS county,
    trim(ownership_type) AS ownership_type,
    try_cast(latitude AS decimal(9,4))  AS latitude,
    try_cast(longitude AS decimal(9,4)) AS longitude
  FROM silver_providerinfo
) s
ON (t.ccn = s.ccn)
WHEN MATCHED THEN UPDATE SET
  provider_name = s.provider_name, state = s.state, city = s.city, county = s.county,
  ownership_type = s.ownership_type, latitude = s.latitude, longitude = s.longitude
WHEN NOT MATCHED THEN INSERT VALUES
  (s.ccn, s.provider_name, s.state, s.city, s.county, s.ownership_type, s.latitude, s.longitude);
