SELECT
  "public"."ed_consumer_price_index"."id" AS "id",
  "public"."ed_consumer_price_index"."year" AS "year",
  "public"."ed_consumer_price_index"."month" AS "month",
  "public"."ed_consumer_price_index"."index" AS "index",
  "public"."ed_consumer_price_index"."year_over_year" AS "year_over_year",
  "public"."ed_consumer_price_index"."effective_dt" AS "effective_dt",
  "public"."ed_consumer_price_index"."modified_at" AS "modified_at"
FROM
  "public"."ed_consumer_price_index"
LIMIT
  1048575
