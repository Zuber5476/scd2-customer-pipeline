import argparse
import json
import logging
from pathlib import Path

from .generator import generate_dataset
from .pipeline import process_csv
from .report import build_report


def main():
    parser = argparse.ArgumentParser(description="SCD Type 2 customer dimension pipeline")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="Process a source CSV")
    run.add_argument("--input", required=True)
    run.add_argument("--db", default="warehouse/scd2.db")
    gen = sub.add_parser("generate", help="Generate reproducible synthetic data")
    gen.add_argument("--output", default="data/generated/events.csv")
    gen.add_argument("--records", type=int, default=1000)
    gen.add_argument("--seed", type=int, default=42)
    report = sub.add_parser("report", help="Write a reconciliation report")
    report.add_argument("--db", default="warehouse/scd2.db")
    report.add_argument("--output", default="reports/validation_report.json")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.command == "run":
        result = process_csv(args.input, args.db)
    elif args.command == "generate":
        result = {"generated_file": str(generate_dataset(args.output, args.records, args.seed))}
    else:
        result = build_report(args.db, args.output)
    print(json.dumps(result, indent=2))
    if args.command == "run" and result["status"] == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
