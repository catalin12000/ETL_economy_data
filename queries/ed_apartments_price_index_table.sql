SELECT
  "public"."ed_apartments_price_index"."id" AS "id",
  "public"."ed_apartments_price_index"."year" AS "year",
  "public"."ed_apartments_price_index"."quarter" AS "quarter",
  "public"."ed_apartments_price_index"."region" AS "region",
  "public"."ed_apartments_price_index"."index" AS "index",
  "public"."ed_apartments_price_index"."up_to_5_years_old_index" AS "up_to_5_years_old_index",
  "public"."ed_apartments_price_index"."over_5_years_old_index" AS "over_5_years_old_index",
  "public"."ed_apartments_price_index"."effective_dt" AS "effective_dt",
  "public"."ed_apartments_price_index"."modified_at" AS "modified_at",
  "public"."ed_apartments_price_index"."created_at" AS "created_at"
FROM
  "public"."ed_apartments_price_index"
LIMIT
  1048575
