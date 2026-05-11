SELECT
  "public"."ed_housing_finances"."id" AS "id",
  "public"."ed_housing_finances"."year" AS "year",
  "public"."ed_housing_finances"."quarter" AS "quarter",
  "public"."ed_housing_finances"."group" AS "group",
  "public"."ed_housing_finances"."category" AS "category",
  "public"."ed_housing_finances"."value_millions" AS "value_millions",
  "public"."ed_housing_finances"."effective_dt" AS "effective_dt",
  "public"."ed_housing_finances"."modified_at" AS "modified_at",
  "public"."ed_housing_finances"."sub_category" AS "sub_category",
  "public"."ed_housing_finances"."created_at" AS "created_at"
FROM
  "public"."ed_housing_finances"
ORDER BY
  "public"."ed_housing_finances"."year" DESC,
  "public"."ed_housing_finances"."quarter" DESC
LIMIT
  1048575
