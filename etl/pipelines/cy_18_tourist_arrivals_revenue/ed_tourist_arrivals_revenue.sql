SELECT
  "public"."ed_tourist_arrivals_revenue"."id" AS "id",
  "public"."ed_tourist_arrivals_revenue"."year" AS "year",
  "public"."ed_tourist_arrivals_revenue"."month" AS "month",
  "public"."ed_tourist_arrivals_revenue"."arrivals" AS "arrivals",
  "public"."ed_tourist_arrivals_revenue"."revenue_millions" AS "revenue_millions",
  "public"."ed_tourist_arrivals_revenue"."effective_dt" AS "effective_dt",
  "public"."ed_tourist_arrivals_revenue"."modified_at" AS "modified_at"
FROM
  "public"."ed_tourist_arrivals_revenue"
LIMIT
  1048575
