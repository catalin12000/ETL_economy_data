SELECT
  "public"."ed_fdi_activity"."id" AS "id",
  "public"."ed_fdi_activity"."year" AS "year",
  "public"."ed_fdi_activity"."section_code" AS "section_code",
  "public"."ed_fdi_activity"."section_name" AS "section_name",
  "public"."ed_fdi_activity"."subsection_code" AS "subsection_code",
  "public"."ed_fdi_activity"."subsection_name" AS "subsection_name",
  "public"."ed_fdi_activity"."amount" AS "amount",
  "public"."ed_fdi_activity"."effective_dt" AS "effective_dt",
  "public"."ed_fdi_activity"."modified_at" AS "modified_at",
  "public"."ed_fdi_activity"."created_at" AS "created_at"
FROM
  "public"."ed_fdi_activity"
LIMIT
  1048575
