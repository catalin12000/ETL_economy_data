import argparse
from etl.core.runner import run_one, list_pipelines
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
        for pid in list_pipelines():
            run_one(pid)
        generate_dashboard()
    else:
        if not args.pipeline:
            raise SystemExit("Use --pipeline <id>, --all or --dashboard")
        run_one(args.pipeline)

if __name__ == "__main__":
    main()
