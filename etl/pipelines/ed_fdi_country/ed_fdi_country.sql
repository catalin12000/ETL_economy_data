SELECT
  "public"."ed_fdi_country"."id" AS "id",
  "public"."ed_fdi_country"."year" AS "year",
  "public"."ed_fdi_country"."country" AS "country",
  "public"."ed_fdi_country"."area" AS "area",
  "public"."ed_fdi_country"."amount" AS "amount",
  "public"."ed_fdi_country"."continent" AS "continent",
  "public"."ed_fdi_country"."effective_dt" AS "effective_dt",
  "public"."ed_fdi_country"."modified_at" AS "modified_at",
  "public"."ed_fdi_country"."created_at" AS "created_at"
FROM
  "public"."ed_fdi_country"
LIMIT
  1048575
