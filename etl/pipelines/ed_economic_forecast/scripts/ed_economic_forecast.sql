SELECT
  "public"."ed_economic_forecast"."id" AS "id",
  "public"."ed_economic_forecast"."year" AS "year",
  "public"."ed_economic_forecast"."gdp_growth" AS "gdp_growth",
  "public"."ed_economic_forecast"."inflation" AS "inflation",
  "public"."ed_economic_forecast"."unemployment" AS "unemployment",
  "public"."ed_economic_forecast"."general_government_balance" AS "general_government_balance",
  "public"."ed_economic_forecast"."gross_public_debt" AS "gross_public_debt",
  "public"."ed_economic_forecast"."current_account_balance" AS "current_account_balance",
  "public"."ed_economic_forecast"."effective_dt" AS "effective_dt",
  "public"."ed_economic_forecast"."modified_at" AS "modified_at",
  "public"."ed_economic_forecast"."created_at" AS "created_at"
FROM
  "public"."ed_economic_forecast"
LIMIT
  1048575
