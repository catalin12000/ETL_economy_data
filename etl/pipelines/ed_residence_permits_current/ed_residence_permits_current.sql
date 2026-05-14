SELECT
  "public"."ed_residence_permits_current"."id" AS "id",
  "public"."ed_residence_permits_current"."month" AS "month",
  "public"."ed_residence_permits_current"."year" AS "year",
  "public"."ed_residence_permits_current"."work" AS "work",
  "public"."ed_residence_permits_current"."other" AS "other",
  "public"."ed_residence_permits_current"."family_reunion" AS "family_reunion",
  "public"."ed_residence_permits_current"."studies" AS "studies",
  "public"."ed_residence_permits_current"."effective_dt" AS "effective_dt",
  "public"."ed_residence_permits_current"."modified_at" AS "modified_at",
  "public"."ed_residence_permits_current"."created_at" AS "created_at"
FROM
  "public"."ed_residence_permits_current"
LIMIT
  1048575
