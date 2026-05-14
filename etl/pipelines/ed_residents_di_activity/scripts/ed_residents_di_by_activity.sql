SELECT
  "public"."ed_residents_di_by_activity"."id" AS "id",
  "public"."ed_residents_di_by_activity"."year" AS "year",
  "public"."ed_residents_di_by_activity"."section_code" AS "section_code",
  "public"."ed_residents_di_by_activity"."section_name" AS "section_name",
  "public"."ed_residents_di_by_activity"."subsection_code" AS "subsection_code",
  "public"."ed_residents_di_by_activity"."subsection_name" AS "subsection_name",
  "public"."ed_residents_di_by_activity"."amount_millions" AS "amount_millions",
  "public"."ed_residents_di_by_activity"."effective_dt" AS "effective_dt",
  "public"."ed_residents_di_by_activity"."modified_at" AS "modified_at",
  "public"."ed_residents_di_by_activity"."created_at" AS "created_at"
FROM
  "public"."ed_residents_di_by_activity"
ORDER BY
  "public"."ed_residents_di_by_activity"."year" DESC
LIMIT
  1048575
