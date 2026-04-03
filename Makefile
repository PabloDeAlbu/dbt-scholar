.PHONY: parse_results

parse_results:
	python3 scripts/parse_run_results.py target/run_results.json
