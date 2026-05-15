SELECT
  "public"."ed_per_day_expenditure_of_tourists"."id" AS "id",
  "public"."ed_per_day_expenditure_of_tourists"."year" AS "year",
  "public"."ed_per_day_expenditure_of_tourists"."month" AS "month",
  "public"."ed_per_day_expenditure_of_tourists"."country_of_origin" AS "country_of_origin",
  "public"."ed_per_day_expenditure_of_tourists"."average_length_of_stay" AS "average_length_of_stay",
  "public"."ed_per_day_expenditure_of_tourists"."expenditure_per_day" AS "expenditure_per_day",
  "public"."ed_per_day_expenditure_of_tourists"."effective_dt" AS "effective_dt",
  "public"."ed_per_day_expenditure_of_tourists"."modified_at" AS "modified_at"
FROM
  "public"."ed_per_day_expenditure_of_tourists"
LIMIT
  1048575
