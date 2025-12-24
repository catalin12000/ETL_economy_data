import argparse
from etl.core.runner import run_one, list_pipelines

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--pipeline", default=None)
    p.add_argument("--all", action="store_true")
    args = p.parse_args()

    if args.all:
        for pid in list_pipelines():
            run_one(pid)
    else:
        if not args.pipeline:
            raise SystemExit("Use --pipeline <id> or --all")
        run_one(args.pipeline)

if __name__ == "__main__":
    main()
