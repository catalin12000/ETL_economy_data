SELECT
  "public"."ed_services_sector_turnover_monthly_index"."id" AS "id",
  "public"."ed_services_sector_turnover_monthly_index"."year" AS "year",
  "public"."ed_services_sector_turnover_monthly_index"."month" AS "month",
  "public"."ed_services_sector_turnover_monthly_index"."economic_activity" AS "economic_activity",
  "public"."ed_services_sector_turnover_monthly_index"."code" AS "code",
  "public"."ed_services_sector_turnover_monthly_index"."index" AS "index",
  "public"."ed_services_sector_turnover_monthly_index"."effective_dt" AS "effective_dt",
  "public"."ed_services_sector_turnover_monthly_index"."modified_dt" AS "modified_dt",
  "public"."ed_services_sector_turnover_monthly_index"."created_at" AS "created_at",
  "public"."ed_services_sector_turnover_monthly_index"."modified_at" AS "modified_at"
FROM
  "public"."ed_services_sector_turnover_monthly_index"
LIMIT
  1048575
