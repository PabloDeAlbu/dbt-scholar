#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
default_output_dir="${repo_root}/exports/analytics_conicet/$(date +%F)"
output_dir="${1:-${default_output_dir}}"

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

export_csv \
    "oai_record_publication.csv" \
    "SELECT * FROM analytics_conicet.oai_record_publication ORDER BY record_id"

export_csv \
    "publication_summary.csv" \
    "SELECT * FROM analytics_conicet.publication_summary"

export_csv \
    "publication_access_right.csv" \
    "SELECT * FROM analytics_conicet.publication_access_right ORDER BY publication_count DESC, access_right"

export_csv \
    "publication_type.csv" \
    "SELECT * FROM analytics_conicet.publication_type ORDER BY publication_count DESC, publication_type"

export_csv \
    "publication_type_access_right.csv" \
    "SELECT * FROM analytics_conicet.publication_type_access_right ORDER BY publication_type, publication_count DESC, access_right"

export_csv \
    "publication_year_type.csv" \
    "SELECT * FROM analytics_conicet.publication_year_type ORDER BY publication_year, publication_count DESC, publication_type"

export_csv \
    "publication_openaire_match_summary.csv" \
    "SELECT * FROM analytics_conicet.publication_openaire_match_summary ORDER BY publication_count DESC"

export_csv \
    "publication_openaire_top_cited.csv" \
    "SELECT * FROM analytics_conicet.publication_openaire_top_cited ORDER BY citation_rank"

export_csv \
    "publication_openaire_citation_by_subject_area.csv" \
    "SELECT * FROM analytics_conicet.publication_openaire_citation_by_subject_area ORDER BY openaire_citation_count DESC, subject_area"

for file in "${temporary_dir}"/*.csv; do
    mv "${file}" "${output_dir}/"
done

echo "CSV exportados en: ${output_dir}"
