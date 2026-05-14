SELECT
  "public"."ed_residence_permits_aggregate"."id" AS "id",
  "public"."ed_residence_permits_aggregate"."month" AS "month",
  "public"."ed_residence_permits_aggregate"."year" AS "year",
  "public"."ed_residence_permits_aggregate"."eu_citizens_of_greek_origin" AS "eu_citizens_of_greek_origin",
  "public"."ed_residence_permits_aggregate"."third_country_nationals" AS "third_country_nationals",
  "public"."ed_residence_permits_aggregate"."political_refugees" AS "political_refugees",
  "public"."ed_residence_permits_aggregate"."effective_dt" AS "effective_dt",
  "public"."ed_residence_permits_aggregate"."modified_at" AS "modified_at",
  "public"."ed_residence_permits_aggregate"."created_at" AS "created_at"
FROM
  "public"."ed_residence_permits_aggregate"
LIMIT
  1048575
