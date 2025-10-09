-- PBJ: raw CSV external (OpenCSVSerde). All strings in bronze.
DROP TABLE IF EXISTS bronze_pbj_daily_nurse_staffing_q2_2024_csv;
CREATE EXTERNAL TABLE bronze_pbj_daily_nurse_staffing_q2_2024_csv (
  PROVNUM string, PROVNAME string, CITY string, STATE string,
  COUNTY_NAME string, COUNTY_FIPS string, CY_Qtr string, WorkDate string,
  MDScensus string, Hrs_RNDON string, Hrs_RNDON_emp string, Hrs_RNDON_ctr string,
  Hrs_RNadmin string, Hrs_RNadmin_emp string, Hrs_RNadmin_ctr string,
  Hrs_RN string, Hrs_RN_emp string, Hrs_RN_ctr string,
  Hrs_LPNadmin string, Hrs_LPNadmin_emp string, Hrs_LPNadmin_ctr string,
  Hrs_LPN string, Hrs_LPN_emp string, Hrs_LPN_ctr string,
  Hrs_CNA string, Hrs_CNA_emp string, Hrs_CNA_ctr string,
  Hrs_NAtrn string, Hrs_NAtrn_emp string, Hrs_NAtrn_ctr string,
  Hrs_MedAide string, Hrs_MedAide_emp string, Hrs_MedAide_ctr string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar"=",", "quoteChar"="\"", "escapeChar"="\\")
LOCATION 's3://kerok-healthcare-landing/bronze/pbj/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- ProviderInfo: raw CSV external (OpenCSVSerde). Columns normalized to snake_case upstream.
DROP TABLE IF EXISTS bronze_nh_providerinfo_oct2024_csv;
CREATE EXTERNAL TABLE bronze_nh_providerinfo_oct2024_csv (
  cms_certification_number_(ccn) string,
  provider_name string, provider_address string, city_town string, state string, zip_code string,
  telephone_number string, provider_ssa_county_code string, county_parish string, ownership_type string,
  number_of_certified_beds string, average_number_of_residents_per_day string,
  average_number_of_residents_per_day_footnote string, provider_type string, provider_resides_in_hospital string,
  legal_business_name string, date_first_approved_to_provide_medicare_and_medicaid_services string,
  affiliated_entity_name string, affiliated_entity_id string, continuing_care_retirement_community string,
  special_focus_status string, abuse_icon string, most_recent_health_inspection_more_than_2_years_ago string,
  provider_changed_ownership_in_last_12_months string, with_a_resident_and_family_council string,
  automatic_sprinkler_systems_in_all_required_areas string,
  overall_rating string, overall_rating_footnote string,
  health_inspection_rating string, health_inspection_rating_footnote string,
  qm_rating string, qm_rating_footnote string, long_stay_qm_rating string,
  long_stay_qm_rating_footnote string, short_stay_qm_rating string, short_stay_qm_rating_footnote string,
  staffing_rating string, staffing_rating_footnote string, reported_staffing_footnote string,
  physical_therapist_staffing_footnote string,
  reported_nurse_aide_staffing_hours_per_resident_per_day string,
  reported_lpn_staffing_hours_per_resident_per_day string,
  reported_rn_staffing_hours_per_resident_per_day string,
  reported_licensed_staffing_hours_per_resident_per_day string,
  reported_total_nurse_staffing_hours_per_resident_per_day string,
  total_number_of_nurse_staff_hours_per_resident_per_day_on_the_weekend string,
  registered_nurse_hours_per_resident_per_day_on_the_weekend string,
  reported_physical_therapist_staffing_hours_per_resident_per_day string,
  total_nursing_staff_turnover string, total_nursing_staff_turnover_footnote string,
  registered_nurse_turnover string, registered_nurse_turnover_footnote string,
  number_of_administrators_who_have_left_the_nursing_home string,
  administrator_turnover_footnote string, nursing_case_mix_index string, nursing_case_mix_index_ratio string,
  case_mix_nurse_aide_staffing_hours_per_resident_per_day string,
  case_mix_lpn_staffing_hours_per_resident_per_day string,
  case_mix_rn_staffing_hours_per_resident_per_day string,
  case_mix_total_nurse_staffing_hours_per_resident_per_day string,
  case_mix_weekend_total_nurse_staffing_hours_per_resident_per_day string,
  adjusted_nurse_aide_staffing_hours_per_resident_per_day string,
  adjusted_lpn_staffing_hours_per_resident_per_day string,
  adjusted_rn_staffing_hours_per_resident_per_day string,
  adjusted_total_nurse_staffing_hours_per_resident_per_day string,
  adjusted_weekend_total_nurse_staffing_hours_per_resident_per_day string,
  rating_cycle_1_standard_survey_health_date string,
  rating_cycle_1_total_number_of_health_deficiencies string,
  rating_cycle_1_number_of_standard_health_deficiencies string,
  rating_cycle_1_number_of_complaint_health_deficiencies string,
  rating_cycle_1_health_deficiency_score string,
  rating_cycle_1_number_of_health_revisits string,
  rating_cycle_1_health_revisit_score string,
  rating_cycle_1_total_health_score string,
  rating_cycle_2_standard_health_survey_date string,
  rating_cycle_2_total_number_of_health_deficiencies string,
  rating_cycle_2_number_of_standard_health_deficiencies string,
  rating_cycle_2_number_of_complaint_health_deficiencies string,
  rating_cycle_2_health_deficiency_score string,
  rating_cycle_2_number_of_health_revisits string,
  rating_cycle_2_health_revisit_score string,
  rating_cycle_2_total_health_score string,
  rating_cycle_3_standard_health_survey_date string,
  rating_cycle_3_total_number_of_health_deficiencies string,
  rating_cycle_3_number_of_standard_health_deficiencies string,
  rating_cycle_3_number_of_complaint_health_deficiencies string,
  rating_cycle_3_health_deficiency_score string,
  rating_cycle_3_number_of_health_revisits string,
  rating_cycle_3_health_revisit_score string,
  rating_cycle_3_total_health_score string,
  total_weighted_health_survey_score string,
  number_of_facility_reported_incidents string,
  number_of_substantiated_complaints string,
  number_of_citations_from_infection_control_inspections string,
  number_of_fines string, total_amount_of_fines_in_dollars string,
  number_of_payment_denials string, total_number_of_penalties string,
  location string, latitude string, longitude string, geocoding_footnote string, processing_date string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar"=",", "quoteChar"="\"", "escapeChar"="\\")
LOCATION 's3://kerok-healthcare-landing/bronze/providerinfo/'
TBLPROPERTIES ("skip.header.line.count"="1");
