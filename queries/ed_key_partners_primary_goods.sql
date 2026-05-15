SELECT
  "public"."ed_key_partners_primary_goods"."id" AS "id",
  "public"."ed_key_partners_primary_goods"."year" AS "year",
  "public"."ed_key_partners_primary_goods"."imports_value" AS "imports_value",
  "public"."ed_key_partners_primary_goods"."exports_value" AS "exports_value",
  "public"."ed_key_partners_primary_goods"."categories" AS "categories",
  "public"."ed_key_partners_primary_goods"."country" AS "country",
  "public"."ed_key_partners_primary_goods"."codes" AS "codes",
  "public"."ed_key_partners_primary_goods"."effective_dt" AS "effective_dt",
  "public"."ed_key_partners_primary_goods"."modified_at" AS "modified_at",
  "public"."ed_key_partners_primary_goods"."created_at" AS "created_at"
FROM
  "public"."ed_key_partners_primary_goods"
LIMIT
  1048575
