SELECT
  "public"."ed_building_permits_by_no_of_rooms"."id" AS "id",
  "public"."ed_building_permits_by_no_of_rooms"."year" AS "year",
  "public"."ed_building_permits_by_no_of_rooms"."month" AS "month",
  "public"."ed_building_permits_by_no_of_rooms"."number_of_new_dwellings" AS "number_of_new_dwellings",
  "public"."ed_building_permits_by_no_of_rooms"."volume_of_new_dwellings" AS "volume_of_new_dwellings",
  "public"."ed_building_permits_by_no_of_rooms"."surface_of_new_dwellings" AS "surface_of_new_dwellings",
  "public"."ed_building_permits_by_no_of_rooms"."habitable_rooms_of_new_dwellings" AS "habitable_rooms_of_new_dwellings",
  "public"."ed_building_permits_by_no_of_rooms"."volume_of_improvements" AS "volume_of_improvements",
  "public"."ed_building_permits_by_no_of_rooms"."region" AS "region",
  "public"."ed_building_permits_by_no_of_rooms"."regional_unit" AS "regional_unit",
  "public"."ed_building_permits_by_no_of_rooms"."effective_dt" AS "effective_dt",
  "public"."ed_building_permits_by_no_of_rooms"."modified_at" AS "modified_at",
  "public"."ed_building_permits_by_no_of_rooms"."created_at" AS "created_at"
FROM
  "public"."ed_building_permits_by_no_of_rooms"
LIMIT
  1048575
