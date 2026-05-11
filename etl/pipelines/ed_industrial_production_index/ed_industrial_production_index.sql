SELECT
  "public"."ed_industrial_production_index"."id" AS "id",
  "public"."ed_industrial_production_index"."year" AS "year",
  "public"."ed_industrial_production_index"."month" AS "month",
  "public"."ed_industrial_production_index"."overall_index" AS "overall_index",
  "public"."ed_industrial_production_index"."seasonally_adjusted_overall_index" AS "seasonally_adjusted_overall_index",
  "public"."ed_industrial_production_index"."mining_quarrying" AS "mining_quarrying",
  "public"."ed_industrial_production_index"."manufacturing" AS "manufacturing",
  "public"."ed_industrial_production_index"."electricity" AS "electricity",
  "public"."ed_industrial_production_index"."water_supply" AS "water_supply",
  "public"."ed_industrial_production_index"."effective_dt" AS "effective_dt",
  "public"."ed_industrial_production_index"."modified_at" AS "modified_at",
  "public"."ed_industrial_production_index"."created_at" AS "created_at"
FROM
  "public"."ed_industrial_production_index"
ORDER BY
  "public"."ed_industrial_production_index"."year" DESC,
  "public"."ed_industrial_production_index"."month" DESC
LIMIT
  1048575

