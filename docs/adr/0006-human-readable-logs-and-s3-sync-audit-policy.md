# Human-readable logs and S3 Sync audit policy

Pipeline Run Logs are human-readable `.log` files only: skipped Pipelines create no log artifacts, errored Pipelines keep their logs local, and delivered Pipelines Sync the same local log beside the S3 Deliverable using the same `{db_table_name}_{timestamp}` stem. Batch execution writes a local Run Log, S3 Sync writes a separate local Sync Log, and neither local summary log is synced to S3; this keeps S3 scoped to Raw files, Deliverables, and the Pipeline Run Logs that directly explain delivered artifacts.
