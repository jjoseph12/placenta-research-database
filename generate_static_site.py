"""Generate a standalone GitHub Pages build of the Placenta Study Atlas."""

import argparse
import json
import shutil
from pathlib import Path

from flask import render_template

from app import app
from database import (
    _facet_values,
    get_atlas_summary,
    get_db_connection,
    get_entry_evidence,
    get_entry_evidence_summary,
    get_entry_supplement_repairs,
    get_featured_studies,
    get_filter_options,
    search_with_filters,
)


FILTER_KEYS = (
    "organism",
    "data_type",
    "library_strategy",
    "trimester",
    "country",
    "evidence",
)

SEARCH_FIELDS = (
    "gse_id",
    "title",
    "organism",
    "data_type",
    "library_strategy",
    "pregnancy_trimester",
    "pregnancy_complications_collected",
    "pregnancy_complications_list",
    "fetal_complications_list",
    "other_phenotypes",
    "main_topic",
    "sample_country",
    "country",
    "pmid",
    "pmcid",
    "doi",
)


def _normalize_base_path(value):
    path = value.strip()
    if not path.startswith("/"):
        path = f"/{path}"
    if not path.endswith("/"):
        path = f"{path}/"
    return path


def _write_text(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix == ".html":
        content = "\n".join(line.rstrip() for line in content.splitlines()) + "\n"
    path.write_text(content, encoding="utf-8")


def _study_index_record(entry, repository_row):
    country = entry.get("sample_country") or entry.get("country")
    evidence_count = int(repository_row.get("evidence_count") or 0)
    repair_count = int(repository_row.get("repair_count") or 0)
    evidence_filters = []
    if evidence_count:
        evidence_filters.append("linked")
    if repair_count:
        evidence_filters.append("supplement_repair")

    search_text = " ".join(
        str(entry.get(field) or "") for field in SEARCH_FIELDS
    ).casefold()

    return {
        "object_id": entry["object_id"],
        "gse_id": entry.get("gse_id"),
        "title": entry.get("title"),
        "organism": entry.get("organism"),
        "sample_size": entry.get("sample_size"),
        "data_type": entry.get("data_type"),
        "library_strategy": entry.get("library_strategy"),
        "pregnancy_trimester": entry.get("pregnancy_trimester"),
        "sample_country": country,
        "evidence_count": evidence_count,
        "repair_count": repair_count,
        "filters": {
            "organism": _facet_values(entry.get("organism"), "organism"),
            "data_type": _facet_values(entry.get("data_type"), "data_type"),
            "library_strategy": _facet_values(
                entry.get("library_strategy"), "library_strategy"
            ),
            "trimester": _facet_values(
                entry.get("pregnancy_trimester"), "trimester"
            ),
            "country": _facet_values(country, "country"),
            "evidence": evidence_filters,
        },
        "search": search_text,
    }


def generate(output_dir, base_path):
    output = Path(output_dir).resolve()
    if output.exists():
        shutil.rmtree(output)
    output.mkdir(parents=True)

    base_path = _normalize_base_path(base_path)
    page_context = {
        "site_base": base_path,
        "page_suffix": ".html",
        "static_site": True,
    }

    conn = get_db_connection()
    entries = [dict(row) for row in conn.execute(
        "SELECT * FROM geo_metadata ORDER BY object_id"
    ).fetchall()]
    conn.close()
    repository_data = search_with_filters(page=1, per_page=max(len(entries), 1))
    repository_rows = {
        row["object_id"]: row for row in repository_data["results"]
    }
    repository_data["results"] = repository_data["results"][:25]
    repository_data["per_page"] = 25
    repository_data["total_pages"] = (
        repository_data["total"] + 24
    ) // 25

    selected_filters = {key: [] for key in FILTER_KEYS}
    with app.app_context():
        _write_text(
            output / "index.html",
            render_template(
                "index.html",
                atlas=get_atlas_summary(),
                featured_studies=get_featured_studies(),
                filter_options=get_filter_options(),
                active_page="index",
                **page_context,
            ),
        )
        _write_text(
            output / "repository.html",
            render_template(
                "repository.html",
                data=repository_data,
                query="",
                filter_options=get_filter_options(),
                selected_filters=selected_filters,
                active_filter_count=0,
                active_page="repository",
                **page_context,
            ),
        )

        total_entries = max((entry["object_id"] for entry in entries), default=0)
        for index, entry in enumerate(entries, start=1):
            _write_text(
                output / "entry" / f"{entry['object_id']}.html",
                render_template(
                    "entry.html",
                    entry=entry,
                    evidence=get_entry_evidence(entry.get("gse_id")),
                    evidence_summary=get_entry_evidence_summary(entry.get("gse_id")),
                    supplement_repairs=get_entry_supplement_repairs(entry.get("gse_id")),
                    column_info={},
                    active_page="repository",
                    total_entries=total_entries,
                    **page_context,
                ),
            )
            if index % 200 == 0:
                print(f"Generated {index:,} of {len(entries):,} study pages")

    studies = [
        _study_index_record(entry, repository_rows[entry["object_id"]])
        for entry in entries
    ]
    _write_text(
        output / "data" / "studies.json",
        json.dumps(studies, ensure_ascii=False, separators=(",", ":")),
    )
    shutil.copytree(Path(__file__).parent / "static", output / "static")
    _write_text(output / ".nojekyll", "")

    print(f"Generated {len(entries):,} study pages in {output}")
    print(f"GitHub Pages base path: {base_path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="dist")
    parser.add_argument(
        "--base-path",
        default="/placenta-research-database/",
        help="URL path where GitHub Pages will serve the site",
    )
    args = parser.parse_args()
    generate(args.output, args.base_path)


if __name__ == "__main__":
    main()
