SELECT
  "public"."ed_tourists_arrivals_revenue"."id" AS "id",
  "public"."ed_tourists_arrivals_revenue"."year" AS "year",
  "public"."ed_tourists_arrivals_revenue"."quarter" AS "quarter",
  "public"."ed_tourists_arrivals_revenue"."area" AS "area",
  "public"."ed_tourists_arrivals_revenue"."country_of_origin" AS "country_of_origin",
  "public"."ed_tourists_arrivals_revenue"."number_of_travellers_000s" AS "number_of_travellers_000s",
  "public"."ed_tourists_arrivals_revenue"."revenues_by_country_of_origin_millions" AS "revenues_by_country_of_origin_millions",
  "public"."ed_tourists_arrivals_revenue"."effective_dt" AS "effective_dt",
  "public"."ed_tourists_arrivals_revenue"."modified_at" AS "modified_at",
  "public"."ed_tourists_arrivals_revenue"."created_at" AS "created_at"
FROM
  "public"."ed_tourists_arrivals_revenue"
LIMIT
  1048575
