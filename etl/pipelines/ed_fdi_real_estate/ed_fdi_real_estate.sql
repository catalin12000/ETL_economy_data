SELECT
  "public"."ed_fdi_real_estate"."id" AS "id",
  "public"."ed_fdi_real_estate"."year" AS "year",
  "public"."ed_fdi_real_estate"."country" AS "country",
  "public"."ed_fdi_real_estate"."area" AS "area",
  "public"."ed_fdi_real_estate"."amount" AS "amount",
  "public"."ed_fdi_real_estate"."effective_dt" AS "effective_dt",
  "public"."ed_fdi_real_estate"."modified_at" AS "modified_at",
  "public"."ed_fdi_real_estate"."created_at" AS "created_at"
FROM
  "public"."ed_fdi_real_estate"
LIMIT
  1048575
