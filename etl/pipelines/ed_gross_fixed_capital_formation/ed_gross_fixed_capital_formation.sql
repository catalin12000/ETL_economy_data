SELECT
  "public"."ed_gross_fixed_capital_formation"."id" AS "id",
  "public"."ed_gross_fixed_capital_formation"."year" AS "year",
  "public"."ed_gross_fixed_capital_formation"."quarter" AS "quarter",
  "public"."ed_gross_fixed_capital_formation"."seasonally" AS "seasonally",
  "public"."ed_gross_fixed_capital_formation"."total_gross_fixed_capital_formation" AS "total_gross_fixed_capital_formation",
  "public"."ed_gross_fixed_capital_formation"."dwellings" AS "dwellings",
  "public"."ed_gross_fixed_capital_formation"."other_buildings_and_structures" AS "other_buildings_and_structures",
  "public"."ed_gross_fixed_capital_formation"."cultivated_biological_resources" AS "cultivated_biological_resources",
  "public"."ed_gross_fixed_capital_formation"."transport_equipment" AS "transport_equipment",
  "public"."ed_gross_fixed_capital_formation"."information_communication_technology_equipment" AS "information_communication_technology_equipment",
  "public"."ed_gross_fixed_capital_formation"."other_machinery_and_equipment_and_weapon_systems" AS "other_machinery_and_equipment_and_weapon_systems",
  "public"."ed_gross_fixed_capital_formation"."intellectual_property_products" AS "intellectual_property_products",
  "public"."ed_gross_fixed_capital_formation"."effective_dt" AS "effective_dt",
  "public"."ed_gross_fixed_capital_formation"."modified_at" AS "modified_at",
  "public"."ed_gross_fixed_capital_formation"."created_at" AS "created_at"
FROM
  "public"."ed_gross_fixed_capital_formation"
LIMIT
  1048575
