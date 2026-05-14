SELECT
  "public"."ed_economic_sentiment_indicator"."id" AS "id",
  "public"."ed_economic_sentiment_indicator"."year" AS "year",
  "public"."ed_economic_sentiment_indicator"."month" AS "month",
  "public"."ed_economic_sentiment_indicator"."geopolitical_entity" AS "geopolitical_entity",
  "public"."ed_economic_sentiment_indicator"."economic_sentiment_indicator" AS "economic_sentiment_indicator",
  "public"."ed_economic_sentiment_indicator"."effective_dt" AS "effective_dt",
  "public"."ed_economic_sentiment_indicator"."modified_at" AS "modified_at",
  "public"."ed_economic_sentiment_indicator"."created_at" AS "created_at"
FROM
  "public"."ed_economic_sentiment_indicator"
ORDER BY
  "public"."ed_economic_sentiment_indicator"."year" DESC,
  "public"."ed_economic_sentiment_indicator"."month" DESC
LIMIT
  1048575
