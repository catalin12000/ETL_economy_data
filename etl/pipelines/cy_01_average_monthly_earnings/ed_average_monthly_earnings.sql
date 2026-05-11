SELECT
  "public"."ed_average_monthly_earnings"."id" AS "id",
  "public"."ed_average_monthly_earnings"."year" AS "year",
  "public"."ed_average_monthly_earnings"."quarter" AS "quarter",
  "public"."ed_average_monthly_earnings"."sex" AS "sex",
  "public"."ed_average_monthly_earnings"."avg_monthly_earnings_unadjusted" AS "avg_monthly_earnings_unadjusted",
  "public"."ed_average_monthly_earnings"."avg_monthly_earnings_seasonally_adjusted" AS "avg_monthly_earnings_seasonally_adjusted",
  "public"."ed_average_monthly_earnings"."effective_dt" AS "effective_dt",
  "public"."ed_average_monthly_earnings"."modified_at" AS "modified_at"
FROM
  "public"."ed_average_monthly_earnings"
LIMIT
  1048575

