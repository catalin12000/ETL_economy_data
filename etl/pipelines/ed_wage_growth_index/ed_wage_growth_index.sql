SELECT
  "public"."ed_wage_growth_index"."id" AS "id",
  "public"."ed_wage_growth_index"."year" AS "year",
  "public"."ed_wage_growth_index"."quarter" AS "quarter",
  "public"."ed_wage_growth_index"."mining_and_quarrying" AS "mining_and_quarrying",
  "public"."ed_wage_growth_index"."manufacturing" AS "manufacturing",
  "public"."ed_wage_growth_index"."electricity_gas_steam_air_conditioning_supply" AS "electricity_gas_steam_air_conditioning_supply",
  "public"."ed_wage_growth_index"."water_supply_sewerage_waste_management_remediation_activities" AS "water_supply_sewerage_waste_management_remediation_activities",
  "public"."ed_wage_growth_index"."construction" AS "construction",
  "public"."ed_wage_growth_index"."wholesale_retail_trade_repair_of_motor_vehicles_motorcycles" AS "wholesale_retail_trade_repair_of_motor_vehicles_motorcycles",
  "public"."ed_wage_growth_index"."transportation_and_storage" AS "transportation_and_storage",
  "public"."ed_wage_growth_index"."accommodation_and_food_service_activities" AS "accommodation_and_food_service_activities",
  "public"."ed_wage_growth_index"."information_and_communication" AS "information_and_communication",
  "public"."ed_wage_growth_index"."professional_scientific_and_technical_activities" AS "professional_scientific_and_technical_activities",
  "public"."ed_wage_growth_index"."administrative_and_support_service_activities" AS "administrative_and_support_service_activities",
  "public"."ed_wage_growth_index"."effective_dt" AS "effective_dt",
  "public"."ed_wage_growth_index"."modified_at" AS "modified_at",
  "public"."ed_wage_growth_index"."created_at" AS "created_at"
FROM
  "public"."ed_wage_growth_index"
ORDER BY
  "public"."ed_wage_growth_index"."year" DESC,
  "public"."ed_wage_growth_index"."quarter" DESC
