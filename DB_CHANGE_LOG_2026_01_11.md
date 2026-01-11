# Database Change Log - 2026-01-11

This file documents all rows created or modified in the `athena` database on January 11, 2026.

## Table: ed_consumer_price_index
Error querying table ed_consumer_price_index: (psycopg2.errors.SyntaxError) unterminated quoted identifier at or near "" WHERE created_at >= '2026-01-11'"
LINE 1: ...ated_at FROM "public\ட்டாக"ed_consumer_price_index" WHERE cr...
                                                             ^

[SQL: SELECT year, month, index, created_at FROM "public\ட்டாக"ed_consumer_price_index" WHERE created_at >= '2026-01-11']
(Background on this error at: https://sqlalche.me/e/20/f405)

## Table: ed_employment
Error querying table ed_employment: (psycopg2.errors.SyntaxError) unterminated quoted identifier at or near "" WHERE created_at >= '2026-01-11'"
LINE 1: ...nally, created_at FROM "public\ட்டாக"ed_employment" WHERE cr...
                                                             ^

[SQL: SELECT year, month, seasonally, created_at FROM "public\ட்டாக"ed_employment" WHERE created_at >= '2026-01-11']
(Background on this error at: https://sqlalche.me/e/20/f405)

## Table: ed_eu_consumer_confidence_index
Error querying table ed_eu_consumer_confidence_index: (psycopg2.errors.SyntaxError) unterminated quoted identifier at or near "" WHERE created_at >= '2026-01-11'"
LINE 1: ...FROM "public\ட்டாக"ed_eu_consumer_confidence_index" WHERE cr...
                                                             ^

[SQL: SELECT year, month, geopolitical_entity, created_at FROM "public\ட்டாக"ed_eu_consumer_confidence_index" WHERE created_at >= '2026-01-11']
(Background on this error at: https://sqlalche.me/e/20/f405)

