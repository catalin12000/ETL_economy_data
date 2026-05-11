SELECT
  "public"."ed_residence_permits_application"."id" AS "id",
  "public"."ed_residence_permits_application"."month" AS "month",
  "public"."ed_residence_permits_application"."year" AS "year",
  "public"."ed_residence_permits_application"."issued" AS "issued",
  "public"."ed_residence_permits_application"."rejected" AS "rejected",
  "public"."ed_residence_permits_application"."revoked" AS "revoked",
  "public"."ed_residence_permits_application"."pending" AS "pending",
  "public"."ed_residence_permits_application"."type" AS "type",
  "public"."ed_residence_permits_application"."effective_dt" AS "effective_dt",
  "public"."ed_residence_permits_application"."modified_at" AS "modified_at",
  "public"."ed_residence_permits_application"."created_at" AS "created_at"
FROM
  "public"."ed_residence_permits_application"
LIMIT
  1048575
