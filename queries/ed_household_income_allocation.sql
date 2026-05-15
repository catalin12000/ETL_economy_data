SELECT
  "public"."ed_household_income_allocation"."id" AS "id",
  "public"."ed_household_income_allocation"."year" AS "year",
  "public"."ed_household_income_allocation"."region" AS "region",
  "public"."ed_household_income_allocation"."regional_unit" AS "regional_unit",
  "public"."ed_household_income_allocation"."account" AS "account",
  "public"."ed_household_income_allocation"."transaction" AS "transaction",
  "public"."ed_household_income_allocation"."amount_millions" AS "amount_millions",
  "public"."ed_household_income_allocation"."final_consumption_expenditure_amount_millions" AS "final_consumption_expenditure_amount_millions",
  "public"."ed_household_income_allocation"."effective_dt" AS "effective_dt",
  "public"."ed_household_income_allocation"."modified_at" AS "modified_at",
  "public"."ed_household_income_allocation"."created_at" AS "created_at"
FROM
  "public"."ed_household_income_allocation"
LIMIT
  1048575
