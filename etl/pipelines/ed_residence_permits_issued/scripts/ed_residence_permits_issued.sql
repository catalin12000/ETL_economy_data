SELECT
  "public"."ed_residence_permits_issued"."id" AS "id",
  "public"."ed_residence_permits_issued"."month" AS "month",
  "public"."ed_residence_permits_issued"."year" AS "year",
  "public"."ed_residence_permits_issued"."work" AS "work",
  "public"."ed_residence_permits_issued"."other" AS "other",
  "public"."ed_residence_permits_issued"."family_reunion" AS "family_reunion",
  "public"."ed_residence_permits_issued"."studies" AS "studies",
  "public"."ed_residence_permits_issued"."type" AS "type",
  "public"."ed_residence_permits_issued"."effective_dt" AS "effective_dt",
  "public"."ed_residence_permits_issued"."modified_at" AS "modified_at",
  "public"."ed_residence_permits_issued"."created_at" AS "created_at"
FROM
  "public"."ed_residence_permits_issued"
LIMIT
  1048575
