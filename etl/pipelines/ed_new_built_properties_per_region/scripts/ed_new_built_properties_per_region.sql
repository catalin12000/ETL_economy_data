SELECT
  "public"."ed_new_built_properties_per_region"."id" AS "id",
  "public"."ed_new_built_properties_per_region"."region" AS "region",
  "public"."ed_new_built_properties_per_region"."regional_unit" AS "regional_unit",
  "public"."ed_new_built_properties_per_region"."year" AS "year",
  "public"."ed_new_built_properties_per_region"."month" AS "month",
  "public"."ed_new_built_properties_per_region"."number" AS "number",
  "public"."ed_new_built_properties_per_region"."storeys" AS "storeys",
  "public"."ed_new_built_properties_per_region"."volume" AS "volume",
  "public"."ed_new_built_properties_per_region"."area" AS "area",
  "public"."ed_new_built_properties_per_region"."effective_dt" AS "effective_dt",
  "public"."ed_new_built_properties_per_region"."modified_at" AS "modified_at",
  "public"."ed_new_built_properties_per_region"."created_at" AS "created_at"
FROM
  "public"."ed_new_built_properties_per_region"
LIMIT
  1048575
