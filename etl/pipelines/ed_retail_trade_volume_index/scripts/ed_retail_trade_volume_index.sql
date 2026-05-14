SELECT
  "public"."ed_retail_trade_volume_index"."id" AS "id",
  "public"."ed_retail_trade_volume_index"."year" AS "year",
  "public"."ed_retail_trade_volume_index"."month" AS "month",
  "public"."ed_retail_trade_volume_index"."overall_index" AS "overall_index",
  "public"."ed_retail_trade_volume_index"."overall_index_excl_automotive" AS "overall_index_excl_automotive",
  "public"."ed_retail_trade_volume_index"."food_sector_index" AS "food_sector_index",
  "public"."ed_retail_trade_volume_index"."overall_index_excl_food_sector" AS "overall_index_excl_food_sector",
  "public"."ed_retail_trade_volume_index"."supermarkets_index" AS "supermarkets_index",
  "public"."ed_retail_trade_volume_index"."department_stores_index" AS "department_stores_index",
  "public"."ed_retail_trade_volume_index"."automotive_fuel_index" AS "automotive_fuel_index",
  "public"."ed_retail_trade_volume_index"."food_beverages_tobacco_index" AS "food_beverages_tobacco_index",
  "public"."ed_retail_trade_volume_index"."pharmaceutical_cosmetics_index" AS "pharmaceutical_cosmetics_index",
  "public"."ed_retail_trade_volume_index"."clothing_footwear_index" AS "clothing_footwear_index",
  "public"."ed_retail_trade_volume_index"."furniture_electrical_household_equipment_index" AS "furniture_electrical_household_equipment_index",
  "public"."ed_retail_trade_volume_index"."books_stationary_other_goods_index" AS "books_stationary_other_goods_index",
  "public"."ed_retail_trade_volume_index"."retail_sale_outside_stores_index" AS "retail_sale_outside_stores_index",
  "public"."ed_retail_trade_volume_index"."effective_dt" AS "effective_dt",
  "public"."ed_retail_trade_volume_index"."modified_at" AS "modified_at",
  "public"."ed_retail_trade_volume_index"."created_at" AS "created_at"
FROM
  "public"."ed_retail_trade_volume_index"
ORDER BY
  "public"."ed_retail_trade_volume_index"."year" DESC,
  "public"."ed_retail_trade_volume_index"."month" DESC
LIMIT
  1048575
