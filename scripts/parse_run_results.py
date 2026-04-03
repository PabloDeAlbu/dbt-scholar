#!/usr/bin/env python3
import json
import sys
from collections import Counter
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 2:
        print("uso: parse_run_results.py <path/to/run_results.json>", file=sys.stderr)
        return 1

    path = Path(sys.argv[1])
    if not path.exists():
        print(f"no existe el archivo: {path}", file=sys.stderr)
        return 1

    with path.open() as fh:
        payload = json.load(fh)

    results = payload.get("results", [])
    if not results:
        print("sin resultados en run_results.json")
        return 0

    status_counter = Counter()
    resource_counter = Counter()
    warn_results = []
    error_results = []

    for result in results:
        status = result.get("status", "unknown")
        unique_id = result.get("unique_id", "")
        resource_type = unique_id.split(".")[0] if "." in unique_id else "unknown"
        failures = result.get("failures")
        execution_time = result.get("execution_time", 0)
        message = result.get("message") or ""

        status_counter[status] += 1
        resource_counter[resource_type] += 1

        entry = {
            "unique_id": unique_id,
            "status": status,
            "failures": failures,
            "execution_time": execution_time,
            "message": message.strip(),
        }

        if status == "warn":
            warn_results.append(entry)
        elif status in {"error", "fail"}:
            error_results.append(entry)

    print("Resumen general")
    print(f"- total_results: {len(results)}")
    print(f"- elapsed_time: {payload.get('elapsed_time')}")

    print("\nEstados")
    for status, count in sorted(status_counter.items()):
        print(f"- {status}: {count}")

    print("\nTipos de recurso")
    for resource_type, count in sorted(resource_counter.items()):
        print(f"- {resource_type}: {count}")

    if warn_results:
        print("\nWarnings")
        for result in warn_results:
            print(
                f"- {result['unique_id']} | failures={result['failures']} "
                f"| execution_time={result['execution_time']:.2f}s"
            )
            if result["message"]:
                print(f"  message: {result['message']}")

    if error_results:
        print("\nErrores")
        for result in error_results:
            print(
                f"- {result['unique_id']} | failures={result['failures']} "
                f"| execution_time={result['execution_time']:.2f}s"
            )
            if result["message"]:
                print(f"  message: {result['message']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
