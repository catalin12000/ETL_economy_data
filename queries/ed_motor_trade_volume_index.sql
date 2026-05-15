SELECT
  "public"."ed_motor_trade_turnover_index"."id" AS "id",
  "public"."ed_motor_trade_turnover_index"."year" AS "year",
  "public"."ed_motor_trade_turnover_index"."month" AS "month",
  "public"."ed_motor_trade_turnover_index"."motor_trade_turnover_index" AS "motor_trade_turnover_index",
  "public"."ed_motor_trade_turnover_index"."vehicle_sale_turnover_index" AS "vehicle_sale_turnover_index",
  "public"."ed_motor_trade_turnover_index"."motor_trade_volume_index" AS "motor_trade_volume_index",
  "public"."ed_motor_trade_turnover_index"."vehicle_sale_volume_index" AS "vehicle_sale_volume_index",
  "public"."ed_motor_trade_turnover_index"."effective_dt" AS "effective_dt",
  "public"."ed_motor_trade_turnover_index"."modified_at" AS "modified_at",
  "public"."ed_motor_trade_turnover_index"."created_at" AS "created_at"
FROM
  "public"."ed_motor_trade_turnover_index"
LIMIT
  1048575
