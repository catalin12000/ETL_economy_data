SELECT
  "public"."ed_building_permits"."id" AS "id",
  "public"."ed_building_permits"."year" AS "year",
  "public"."ed_building_permits"."month" AS "month",
  "public"."ed_building_permits"."permits_number" AS "permits_number",
  "public"."ed_building_permits"."area" AS "area",
  "public"."ed_building_permits"."volume" AS "volume",
  "public"."ed_building_permits"."effective_dt" AS "effective_dt",
  "public"."ed_building_permits"."modified_at" AS "modified_at",
  "public"."ed_building_permits"."created_at" AS "created_at"
FROM
  "public"."ed_building_permits"
LIMIT
  1048575
