SELECT
  "public"."ed_residence_permits_top10_countries"."id" AS "id",
  "public"."ed_residence_permits_top10_countries"."month" AS "month",
  "public"."ed_residence_permits_top10_countries"."year" AS "year",
  "public"."ed_residence_permits_top10_countries"."rank" AS "rank",
  "public"."ed_residence_permits_top10_countries"."country" AS "country",
  "public"."ed_residence_permits_top10_countries"."total_permits_granted" AS "total_permits_granted",
  "public"."ed_residence_permits_top10_countries"."permits_granted_to_men" AS "permits_granted_to_men",
  "public"."ed_residence_permits_top10_countries"."permits_granted_to_women" AS "permits_granted_to_women",
  "public"."ed_residence_permits_top10_countries"."effective_dt" AS "effective_dt",
  "public"."ed_residence_permits_top10_countries"."modified_at" AS "modified_at",
  "public"."ed_residence_permits_top10_countries"."created_at" AS "created_at"
FROM
  "public"."ed_residence_permits_top10_countries"
LIMIT
  1048575
