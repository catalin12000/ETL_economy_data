SELECT
  "public"."ed_residents_di_by_country"."id" AS "id",
  "public"."ed_residents_di_by_country"."year" AS "year",
  "public"."ed_residents_di_by_country"."country" AS "country",
  "public"."ed_residents_di_by_country"."area" AS "area",
  "public"."ed_residents_di_by_country"."amount_millions" AS "amount_millions",
  "public"."ed_residents_di_by_country"."continent" AS "continent",
  "public"."ed_residents_di_by_country"."effective_dt" AS "effective_dt",
  "public"."ed_residents_di_by_country"."modified_at" AS "modified_at",
  "public"."ed_residents_di_by_country"."created_at" AS "created_at"
FROM
  "public"."ed_residents_di_by_country"
ORDER BY
  "public"."ed_residents_di_by_country"."year" DESC
LIMIT
  1048575
