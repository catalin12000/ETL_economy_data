SELECT
  "public"."ed_economic_forecast_cy"."id" AS "id",
  "public"."ed_economic_forecast_cy"."year" AS "year",
  "public"."ed_economic_forecast_cy"."gdp_growth" AS "gdp_growth",
  "public"."ed_economic_forecast_cy"."inflation" AS "inflation",
  "public"."ed_economic_forecast_cy"."unemployment" AS "unemployment",
  "public"."ed_economic_forecast_cy"."general_government_balance" AS "general_government_balance",
  "public"."ed_economic_forecast_cy"."gross_public_debt" AS "gross_public_debt",
  "public"."ed_economic_forecast_cy"."current_account_balance" AS "current_account_balance",
  "public"."ed_economic_forecast_cy"."effective_dt" AS "effective_dt",
  "public"."ed_economic_forecast_cy"."modified_at" AS "modified_at"
FROM
  "public"."ed_economic_forecast_cy"
LIMIT
  1048575
