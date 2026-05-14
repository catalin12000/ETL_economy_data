SELECT
  "public"."ed_geo_distribution_of_issued_and_pending_permits"."id" AS "id",
  "public"."ed_geo_distribution_of_issued_and_pending_permits"."year" AS "year",
  "public"."ed_geo_distribution_of_issued_and_pending_permits"."month" AS "month",
  "public"."ed_geo_distribution_of_issued_and_pending_permits"."permit_type" AS "permit_type",
  "public"."ed_geo_distribution_of_issued_and_pending_permits"."period" AS "period",
  "public"."ed_geo_distribution_of_issued_and_pending_permits"."area" AS "area",
  "public"."ed_geo_distribution_of_issued_and_pending_permits"."issued" AS "issued",
  "public"."ed_geo_distribution_of_issued_and_pending_permits"."rejected" AS "rejected",
  "public"."ed_geo_distribution_of_issued_and_pending_permits"."revoked" AS "revoked",
  "public"."ed_geo_distribution_of_issued_and_pending_permits"."pending" AS "pending",
  "public"."ed_geo_distribution_of_issued_and_pending_permits"."effective_dt" AS "effective_dt",
  "public"."ed_geo_distribution_of_issued_and_pending_permits"."modified_at" AS "modified_at",
  "public"."ed_geo_distribution_of_issued_and_pending_permits"."created_at" AS "created_at"
FROM
  "public"."ed_geo_distribution_of_issued_and_pending_permits"
LIMIT
  1048575
