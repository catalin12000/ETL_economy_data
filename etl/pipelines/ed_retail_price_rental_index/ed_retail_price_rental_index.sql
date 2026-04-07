SELECT
  "public"."ed_retail_price_rental_index"."id" AS "id",
  "public"."ed_retail_price_rental_index"."year" AS "year",
  "public"."ed_retail_price_rental_index"."year_half" AS "year_half",
  "public"."ed_retail_price_rental_index"."total_price_index" AS "total_price_index",
  "public"."ed_retail_price_rental_index"."total_rent_index" AS "total_rent_index",
  "public"."ed_retail_price_rental_index"."athens_price_index" AS "athens_price_index",
  "public"."ed_retail_price_rental_index"."athens_rent_index" AS "athens_rent_index",
  "public"."ed_retail_price_rental_index"."thessaloniki_price_index" AS "thessaloniki_price_index",
  "public"."ed_retail_price_rental_index"."thessaloniki_rent_index" AS "thessaloniki_rent_index",
  "public"."ed_retail_price_rental_index"."rest_of_greece_price_index" AS "rest_of_greece_price_index",
  "public"."ed_retail_price_rental_index"."rest_of_greece_rent_index" AS "rest_of_greece_rent_index",
  "public"."ed_retail_price_rental_index"."effective_dt" AS "effective_dt",
  "public"."ed_retail_price_rental_index"."modified_at" AS "modified_at",
  "public"."ed_retail_price_rental_index"."created_at" AS "created_at"
FROM
  "public"."ed_retail_price_rental_index"
ORDER BY
  "public"."ed_retail_price_rental_index"."year" DESC
LIMIT
  1048575
