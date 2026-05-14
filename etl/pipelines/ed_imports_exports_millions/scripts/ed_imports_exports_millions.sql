SELECT
  "public"."ed_imports_exports_millions"."id" AS "id",
  "public"."ed_imports_exports_millions"."year" AS "year",
  "public"."ed_imports_exports_millions"."current_prices_goods" AS "current_prices_goods",
  "public"."ed_imports_exports_millions"."current_prices_services" AS "current_prices_services",
  "public"."ed_imports_exports_millions"."current_prices_imports" AS "current_prices_imports",
  "public"."ed_imports_exports_millions"."current_prices_expenditures_of_residents_in_rest_of_the_world" AS "current_prices_expenditures_of_residents_in_rest_of_the_world",
  "public"."ed_imports_exports_millions"."current_prices_goods_exports" AS "current_prices_goods_exports",
  "public"."ed_imports_exports_millions"."current_prices_services_exports" AS "current_prices_services_exports",
  "public"."ed_imports_exports_millions"."current_prices_exports" AS "current_prices_exports",
  "public"."ed_imports_exports_millions"."current_prices_expenditures_of_residents_on_economic_territory" AS "current_prices_expenditures_of_residents_on_economic_territory",
  "public"."ed_imports_exports_millions"."current_prices_exports_imports_balance" AS "current_prices_exports_imports_balance",
  "public"."ed_imports_exports_millions"."constant_prices_goods" AS "constant_prices_goods",
  "public"."ed_imports_exports_millions"."constant_prices_services" AS "constant_prices_services",
  "public"."ed_imports_exports_millions"."constant_prices_imports" AS "constant_prices_imports",
  "public"."ed_imports_exports_millions"."constant_prices_expenditures_of_residents_in_rest_of_the_world" AS "constant_prices_expenditures_of_residents_in_rest_of_the_world",
  "public"."ed_imports_exports_millions"."constant_prices_goods_exports" AS "constant_prices_goods_exports",
  "public"."ed_imports_exports_millions"."constant_prices_services_exports" AS "constant_prices_services_exports",
  "public"."ed_imports_exports_millions"."constant_prices_exports" AS "constant_prices_exports",
  "public"."ed_imports_exports_millions"."constant_prices_expenditures_of_residents_on_economic_territory" AS "constant_prices_expenditures_of_residents_on_economic_territory",
  "public"."ed_imports_exports_millions"."constant_prices_exports_imports_balance" AS "constant_prices_exports_imports_balance",
  "public"."ed_imports_exports_millions"."effective_dt" AS "effective_dt",
  "public"."ed_imports_exports_millions"."modified_at" AS "modified_at",
  "public"."ed_imports_exports_millions"."created_at" AS "created_at"
FROM
  "public"."ed_imports_exports_millions"
ORDER BY
  "public"."ed_imports_exports_millions"."year" DESC
LIMIT
  1048575

