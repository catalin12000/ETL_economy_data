SELECT
  "public"."ed_gross_value_added_sector"."id" AS "id",
  "public"."ed_gross_value_added_sector"."year" AS "year",
  "public"."ed_gross_value_added_sector"."total_economy" AS "total_economy",
  "public"."ed_gross_value_added_sector"."agriculture_forestry_fishing" AS "agriculture_forestry_fishing",
  "public"."ed_gross_value_added_sector"."mining_quarrying" AS "mining_quarrying",
  "public"."ed_gross_value_added_sector"."manufacturing" AS "manufacturing",
  "public"."ed_gross_value_added_sector"."electricity_gas_steam_ac_supply" AS "electricity_gas_steam_ac_supply",
  "public"."ed_gross_value_added_sector"."water_supply_sewerage_waste_mgmt" AS "water_supply_sewerage_waste_mgmt",
  "public"."ed_gross_value_added_sector"."construction" AS "construction",
  "public"."ed_gross_value_added_sector"."wholesale_retail_trade_motor_repair" AS "wholesale_retail_trade_motor_repair",
  "public"."ed_gross_value_added_sector"."transportation_storage" AS "transportation_storage",
  "public"."ed_gross_value_added_sector"."accommodation_food_services" AS "accommodation_food_services",
  "public"."ed_gross_value_added_sector"."info_communication" AS "info_communication",
  "public"."ed_gross_value_added_sector"."financial_insurance_activities" AS "financial_insurance_activities",
  "public"."ed_gross_value_added_sector"."real_estate_activities" AS "real_estate_activities",
  "public"."ed_gross_value_added_sector"."prof_sci_tech_activities" AS "prof_sci_tech_activities",
  "public"."ed_gross_value_added_sector"."admin_support_services_activities" AS "admin_support_services_activities",
  "public"."ed_gross_value_added_sector"."public_admin_defence_social_sec" AS "public_admin_defence_social_sec",
  "public"."ed_gross_value_added_sector"."education" AS "education",
  "public"."ed_gross_value_added_sector"."human_health_social_work_activities" AS "human_health_social_work_activities",
  "public"."ed_gross_value_added_sector"."arts_entertainment_recreation" AS "arts_entertainment_recreation",
  "public"."ed_gross_value_added_sector"."other_services_activities" AS "other_services_activities",
  "public"."ed_gross_value_added_sector"."household_employer_activities" AS "household_employer_activities",
  "public"."ed_gross_value_added_sector"."effective_dt" AS "effective_dt",
  "public"."ed_gross_value_added_sector"."modified_at" AS "modified_at"
FROM
  "public"."ed_gross_value_added_sector"
LIMIT
  1048575
