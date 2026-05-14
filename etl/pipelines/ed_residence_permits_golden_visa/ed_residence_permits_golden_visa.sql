SELECT
  "public"."ed_residence_permits_golden_visa"."id" AS "id",
  "public"."ed_residence_permits_golden_visa"."month" AS "month",
  "public"."ed_residence_permits_golden_visa"."year" AS "year",
  "public"."ed_residence_permits_golden_visa"."issued" AS "issued",
  "public"."ed_residence_permits_golden_visa"."rejected" AS "rejected",
  "public"."ed_residence_permits_golden_visa"."revoked" AS "revoked",
  "public"."ed_residence_permits_golden_visa"."pending" AS "pending",
  "public"."ed_residence_permits_golden_visa"."type" AS "type",
  "public"."ed_residence_permits_golden_visa"."effective_dt" AS "effective_dt",
  "public"."ed_residence_permits_golden_visa"."modified_at" AS "modified_at",
  "public"."ed_residence_permits_golden_visa"."created_at" AS "created_at"
FROM
  "public"."ed_residence_permits_golden_visa"
LIMIT
  1048575
