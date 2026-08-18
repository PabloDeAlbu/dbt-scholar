EXPORTS_DIR ?= var/exports
DATE ?= $(shell date +%F)
TARGET ?= dev_docker
DW_CONTAINER ?= dw-scholar_postgres
DW_USER ?= dw_user
DW_DB ?= dw
EXPORT_FILE = $(EXPORTS_DIR)/$(MODEL)_$(DATE).csv

.PHONY: export

export:
	@if [ -z "$(MODEL)" ]; then \
		echo "Uso: make export MODEL=<nombre_modelo> [TARGET=dev_docker] [DATE=YYYY-MM-DD]" >&2; \
		exit 2; \
	fi
	@mkdir -p $(EXPORTS_DIR)
	@relation="$$(dbt --quiet ls \
		--target $(TARGET) \
		--select $(MODEL) \
		--resource-type model \
		--output json \
		--output-keys name relation_name \
		| jq -r 'select(.relation_name != null) | .relation_name')"; \
	if [ -z "$$relation" ]; then \
		echo "No se encontró el modelo dbt: $(MODEL)" >&2; \
		exit 2; \
	fi; \
	if [ "$$(printf '%s\n' "$$relation" | wc -l)" -ne 1 ]; then \
		echo "El selector debe resolver un único modelo: $(MODEL)" >&2; \
		exit 2; \
	fi; \
	docker exec -i $(DW_CONTAINER) \
		psql -U $(DW_USER) -d $(DW_DB) \
		-c "COPY (SELECT * FROM $$relation) TO STDOUT WITH (FORMAT CSV, HEADER TRUE)" \
		> $(EXPORT_FILE)
	@echo "Export generado en $(EXPORT_FILE)"
