SELECT
  "public"."ed_wholesales_turnover_index"."id" AS "id",
  "public"."ed_wholesales_turnover_index"."year" AS "year",
  "public"."ed_wholesales_turnover_index"."month" AS "month",
  "public"."ed_wholesales_turnover_index"."turnover_index" AS "turnover_index",
  "public"."ed_wholesales_turnover_index"."volume_index" AS "volume_index",
  "public"."ed_wholesales_turnover_index"."effective_dt" AS "effective_dt",
  "public"."ed_wholesales_turnover_index"."modified_at" AS "modified_at",
  "public"."ed_wholesales_turnover_index"."created_at" AS "created_at"
FROM
  "public"."ed_wholesales_turnover_index"
LIMIT
  1048575
