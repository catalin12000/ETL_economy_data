SELECT
  "public"."ed_new_establishments_building_permits"."id" AS "id",
  "public"."ed_new_establishments_building_permits"."year" AS "year",
  "public"."ed_new_establishments_building_permits"."month" AS "month",
  "public"."ed_new_establishments_building_permits"."regional_unit" AS "regional_unit",
  "public"."ed_new_establishments_building_permits"."area_type" AS "area_type",
  "public"."ed_new_establishments_building_permits"."category_of_use" AS "category_of_use",
  "public"."ed_new_establishments_building_permits"."number" AS "number",
  "public"."ed_new_establishments_building_permits"."volume" AS "volume",
  "public"."ed_new_establishments_building_permits"."effective_dt" AS "effective_dt",
  "public"."ed_new_establishments_building_permits"."modified_at" AS "modified_at",
  "public"."ed_new_establishments_building_permits"."created_at" AS "created_at"
FROM
  "public"."ed_new_establishments_building_permits"
LIMIT
  1048575
