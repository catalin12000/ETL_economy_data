SELECT
  "public"."ed_construction_index"."id" AS "id",
  "public"."ed_construction_index"."year" AS "year",
  "public"."ed_construction_index"."quarter" AS "quarter",
  "public"."ed_construction_index"."production_index_in_construction" AS "production_index_in_construction",
  "public"."ed_construction_index"."production_index_building_construction" AS "production_index_building_construction",
  "public"."ed_construction_index"."production_index_civil_engineering" AS "production_index_civil_engineering",
  "public"."ed_construction_index"."effective_dt" AS "effective_dt",
  "public"."ed_construction_index"."modified_at" AS "modified_at",
  "public"."ed_construction_index"."created_at" AS "created_at"
FROM
  "public"."ed_construction_index"
LIMIT
  1048575
