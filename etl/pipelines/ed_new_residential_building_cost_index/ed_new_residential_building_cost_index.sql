WITH ranked AS (
  SELECT
    "public"."ed_new_residential_building_cost_index"."id" AS "id",
    "public"."ed_new_residential_building_cost_index"."year" AS "year",
    "public"."ed_new_residential_building_cost_index"."quarter" AS "quarter",
    "public"."ed_new_residential_building_cost_index"."overall_cost_index" AS "overall_cost_index",
    "public"."ed_new_residential_building_cost_index"."material_costs_index" AS "material_costs_index",
    "public"."ed_new_residential_building_cost_index"."labour_costs_index" AS "labour_costs_index",
    "public"."ed_new_residential_building_cost_index"."effective_dt" AS "effective_dt",
    "public"."ed_new_residential_building_cost_index"."modified_at" AS "modified_at",
    "public"."ed_new_residential_building_cost_index"."created_at" AS "created_at",
    ROW_NUMBER() OVER (
      PARTITION BY
        "public"."ed_new_residential_building_cost_index"."year",
        "public"."ed_new_residential_building_cost_index"."quarter"
      ORDER BY
        "public"."ed_new_residential_building_cost_index"."modified_at" DESC NULLS LAST,
        "public"."ed_new_residential_building_cost_index"."created_at" DESC NULLS LAST,
        "public"."ed_new_residential_building_cost_index"."id" DESC
    ) AS rn
  FROM
    "public"."ed_new_residential_building_cost_index"
)
SELECT
  "id",
  "year",
  "quarter",
  "overall_cost_index",
  "material_costs_index",
  "labour_costs_index",
  "effective_dt",
  "modified_at",
  "created_at"
FROM ranked
WHERE rn = 1
