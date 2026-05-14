import argparse

from etl.core.runner import list_pipelines, print_run_summary, run_one
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
        results = [run_one(pid) for pid in list_pipelines()]
        print_run_summary(results)
        generate_dashboard()
        return

    if not args.pipeline:
        raise SystemExit("Use --pipeline <id>, --all or --dashboard")

    print_run_summary([run_one(args.pipeline)])


if __name__ == "__main__":
    main()
