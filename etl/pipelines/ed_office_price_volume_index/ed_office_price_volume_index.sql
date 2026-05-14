SELECT
  "public"."ed_office_price_volume_index"."id" AS "id",
  "public"."ed_office_price_volume_index"."year" AS "year",
  "public"."ed_office_price_volume_index"."year_half" AS "year_half",
  "public"."ed_office_price_volume_index"."total_price_index" AS "total_price_index",
  "public"."ed_office_price_volume_index"."total_rent_index" AS "total_rent_index",
  "public"."ed_office_price_volume_index"."athens_price_index" AS "athens_price_index",
  "public"."ed_office_price_volume_index"."athens_rent_index" AS "athens_rent_index",
  "public"."ed_office_price_volume_index"."thessaloniki_price_index" AS "thessaloniki_price_index",
  "public"."ed_office_price_volume_index"."thessaloniki_rent_index" AS "thessaloniki_rent_index",
  "public"."ed_office_price_volume_index"."rest_of_greece_price_index" AS "rest_of_greece_price_index",
  "public"."ed_office_price_volume_index"."rest_of_greece_rent_index" AS "rest_of_greece_rent_index"
FROM
  "public"."ed_office_price_volume_index"
ORDER BY
  "public"."ed_office_price_volume_index"."year",
  "public"."ed_office_price_volume_index"."year_half"
