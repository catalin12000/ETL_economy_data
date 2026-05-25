import argparse
from datetime import datetime, timezone

from etl.core.pipeline_logging import new_run_id
from etl.core.runner import list_pipelines, print_run_summary, run_one, write_run_log
from dashboard import generate_dashboard


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--pipeline", default=None)
    p.add_argument("--all", action="store_true")
    p.add_argument("--dashboard", action="store_true")
    args = p.parse_args()

    if args.dashboard:
        generate_dashboard()
        return

    if args.all:
        run_id = new_run_id()
        started_at = datetime.now(timezone.utc)
        pipelines = list_pipelines()
        results = [run_one(pid, run_id=run_id) for pid in pipelines]
        print_run_summary(results)
        generate_dashboard()
        write_run_log(run_id, results, mode="all", started_at=started_at)
        return

    if not args.pipeline:
        raise SystemExit("Use --pipeline <id>, --all or --dashboard")

    run_id = new_run_id()
    started_at = datetime.now(timezone.utc)
    results = [run_one(args.pipeline, run_id=run_id)]
    print_run_summary(results)
    write_run_log(run_id, results, mode="single", started_at=started_at)


if __name__ == "__main__":
    main()
