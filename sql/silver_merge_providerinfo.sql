MERGE INTO silver_providerinfo t
USING (
  SELECT
    -- CCN normalize: keep rightmost 6 digits, left-pad zeros
    lpad(substr(regexp_replace(trim(ccn_raw),'[^0-9]',''),
         greatest(length(regexp_replace(trim(ccn_raw),'[^0-9]',''))-5,0)+1), 6, '0') AS ccn,

    upper(trim(state)) AS state,
    trim(provider_name) AS provider_name,
    trim(provider_address) AS provider_address,
    trim(city_town) AS city,
    trim(county_parish) AS county,
    trim(ownership_type) AS ownership_type,
    trim(zip_code) AS zip_code,
    trim(telephone_number) AS telephone_number,
    trim(provider_ssa_county_code) AS provider_ssa_county_code,

    try_cast(nullif(latitude,'') AS decimal(9,4)) AS latitude,
    try_cast(nullif(longitude,'') AS decimal(9,4)) AS longitude,
    trim(location) AS facility_location,
    trim(geocoding_footnote) AS geocoding_footnote,

    try_cast(number_of_certified_beds AS integer) AS number_of_certified_beds,
    try_cast(average_number_of_residents_per_day AS double) AS average_number_of_residents_per_day,

    -- keep common performance/ratings you’ll chart later (add more if needed)
    try_cast(reported_total_nurse_staffing_hours_per_resident_per_day AS double) AS reported_total_nurse_staffing_hours_per_resident_per_day,
    try_cast(adjusted_total_nurse_staffing_hours_per_resident_per_day AS double)    AS adjusted_total_nurse_staffing_hours_per_resident_per_day,
    try_cast(number_of_fines AS integer) AS number_of_fines,
    try_cast(total_amount_of_fines_in_dollars AS double) AS total_amount_of_fines_in_dollars,
    try_cast(number_of_payment_denials AS integer) AS number_of_payment_denials,
    try_cast(total_number_of_penalties AS integer) AS total_number_of_penalties,

    -- leave dates as strings for now per prior decision
    processing_date

  FROM (
    SELECT
      "cms_certification_number_(ccn)" AS ccn_raw, state, provider_name, provider_address, city_town,
      county_parish, ownership_type, zip_code, telephone_number, provider_ssa_county_code,
      latitude, longitude, location, geocoding_footnote,
      number_of_certified_beds, average_number_of_residents_per_day,
      reported_total_nurse_staffing_hours_per_resident_per_day,
      adjusted_total_nurse_staffing_hours_per_resident_per_day,
      number_of_fines, total_amount_of_fines_in_dollars,
      number_of_payment_denials, total_number_of_penalties, processing_date
    FROM bronze_nh_providerinfo_oct2024_csv
    WHERE "$path" = :source_path
  ) b
) s
ON (t.ccn = s.ccn)
WHEN MATCHED THEN UPDATE SET
  state = coalesce(s.state, t.state),
  provider_name = coalesce(s.provider_name, t.provider_name),
  provider_address = coalesce(s.provider_address, t.provider_address),
  city = coalesce(s.city, t.city),
  county = coalesce(s.county, t.county),
  ownership_type = coalesce(s.ownership_type, t.ownership_type),
  zip_code = coalesce(s.zip_code, t.zip_code),
  telephone_number = coalesce(s.telephone_number, t.telephone_number),
  provider_ssa_county_code = coalesce(s.provider_ssa_county_code, t.provider_ssa_county_code),
  latitude = coalesce(s.latitude, t.latitude),
  longitude = coalesce(s.longitude, t.longitude),
  facility_location = coalesce(s.facility_location, t.facility_location),
  geocoding_footnote = coalesce(s.geocoding_footnote, t.geocoding_footnote),
  number_of_certified_beds = coalesce(s.number_of_certified_beds, t.number_of_certified_beds),
  average_number_of_residents_per_day = coalesce(s.average_number_of_residents_per_day, t.average_number_of_residents_per_day),
  reported_total_nurse_staffing_hours_per_resident_per_day = coalesce(s.reported_total_nurse_staffing_hours_per_resident_per_day, t.reported_total_nurse_staffing_hours_per_resident_per_day),
  adjusted_total_nurse_staffing_hours_per_resident_per_day = coalesce(s.adjusted_total_nurse_staffing_hours_per_resident_per_day, t.adjusted_total_nurse_staffing_hours_per_resident_per_day),
  number_of_fines = coalesce(s.number_of_fines, t.number_of_fines),
  total_amount_of_fines_in_dollars = coalesce(s.total_amount_of_fines_in_dollars, t.total_amount_of_fines_in_dollars),
  number_of_payment_denials = coalesce(s.number_of_payment_denials, t.number_of_payment_denials),
  total_number_of_penalties = coalesce(s.total_number_of_penalties, t.total_number_of_penalties),
  processing_date = coalesce(s.processing_date, t.processing_date)
WHEN NOT MATCHED THEN INSERT VALUES (
  s.ccn, s.provider_name, s.provider_address, s.city, s.state, s.zip_code,
  s.telephone_number, s.ownership_type, s.county, s.provider_ssa_county_code,
  s.number_of_certified_beds, s.average_number_of_residents_per_day,
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,            -- placeholders for long list if you widen later
  s.facility_location, s.latitude, s.longitude, s.geocoding_footnote, s.processing_date
);
