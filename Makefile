DB_CONTAINER ?= dspacedb-1
PGUSER ?= dspace
SOURCE_DB ?= dspace
TARGET_DB ?= dw
DUMPS_DIR ?= var/dumps
DATE ?= $(shell date +%F)
DSPACE_DUMP ?= $(DUMPS_DIR)/dspace_$(DATE).sql

.PHONY: parse_results dump-dspace-public restore-dspace-public-to-dw print-dspace-dump-path

parse_results:
	python3 scripts/parse_run_results.py target/run_results.json

print-dspace-dump-path:
	@echo $(DSPACE_DUMP)

dump-dspace-public:
	@mkdir -p $(DUMPS_DIR)
	docker exec -t $(DB_CONTAINER) sh -lc 'pg_dump -U $(PGUSER) -d $(SOURCE_DB) --schema=public --no-owner --no-privileges' > $(DSPACE_DUMP)
	@echo "Dump generado en $(DSPACE_DUMP)"

restore-dspace-public-to-dw:
	psql -h localhost -p 5432 -U $(PGUSER) -d $(TARGET_DB) < $(DSPACE_DUMP)
	@echo "Restore completado en $(TARGET_DB) desde $(DSPACE_DUMP)"
