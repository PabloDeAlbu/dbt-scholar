#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
    echo "Uso: $0 <target_community_id> [directorio_salida]" >&2
    exit 2
fi

target_community_id="$1"
if [[ ! "${target_community_id}" =~ ^[0-9]+$ ]]; then
    echo "Error: target_community_id debe ser un entero positivo." >&2
    exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
default_output_dir="${repo_root}/exports/analytics_unlp/dedup/conicet/$(date +%F)/community_${target_community_id}"
output_dir="${2:-${default_output_dir}}"

mkdir -p "${output_dir}"
output_dir="$(cd "${output_dir}" && pwd -P)"

if [[ "${output_dir}" == *"'"* ]]; then
    echo "Error: el directorio de salida no puede contener comillas simples." >&2
    exit 2
fi

temporary_dir="$(mktemp -d "${output_dir}/.export.XXXXXX")"
trap 'rm -rf "${temporary_dir}"' EXIT

psql_args=(-X --set=ON_ERROR_STOP=1)
if [[ -n "${DATABASE_URL:-}" ]]; then
    psql_args=("${DATABASE_URL}" "${psql_args[@]}")
fi

export_csv() {
    local filename="$1"
    local query="$2"
    local temporary_file="${temporary_dir}/${filename}"

    psql "${psql_args[@]}" \
        --command="\\copy (${query}) TO '${temporary_file}' WITH (FORMAT CSV, HEADER true, ENCODING 'UTF8')"
}

dedup_columns="id, title, subtitle, type, author, date, doi, isbn, issn, description"

export_csv \
    "input_1.csv" \
    "SELECT ${dedup_columns} FROM analytics_unlp.dedup_conicet_unlp_input_1 WHERE dedup_eligible AND target_community_id = ${target_community_id} ORDER BY id"

export_csv \
    "input_2.csv" \
    "SELECT ${dedup_columns} FROM analytics_unlp.dedup_conicet_unlp_input_2 WHERE target_community_id = ${target_community_id} ORDER BY id"

for file in "${temporary_dir}"/*.csv; do
    mv "${file}" "${output_dir}/"
done

echo "Inputs del deduplicador exportados en: ${output_dir}"
