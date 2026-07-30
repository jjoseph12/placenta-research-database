"""
Database utilities for the Placenta Study Atlas.

The site is intentionally backed by the existing SQLite file so the first
product pass can focus on better discovery, filtering, and presentation.
"""

import re
import sqlite3
from collections import Counter
from pathlib import Path

DB_PATH = Path(__file__).parent / "geo_metadata.db"

FILTER_COLUMNS = {
    "organism": "organism",
    "data_type": "data_type",
    "library_strategy": "library_strategy",
    "trimester": "pregnancy_trimester",
    "country": "sample_country",
}

# GEO stores several filterable fields as comma-separated display strings.  The
# atlas preserves those source strings, but filters operate on their individual
# values so a multi-assay study remains discoverable under each assay.
MULTI_VALUE_FACETS = {"data_type", "library_strategy", "trimester", "country"}

COUNTRY_ALIASES = {
    "u.s.": "United States",
    "u.s.a.": "United States",
    "usa": "United States",
    "united states": "United States",
    "united states of america": "United States",
    "uk": "United Kingdom",
    "u.k.": "United Kingdom",
    "united kingdom": "United Kingdom",
}

TRIMESTER_ALIASES = {
    "first": "1st",
    "first trimester": "1st",
    "1st": "1st",
    "1st trimester": "1st",
    "second": "2nd",
    "second trimester": "2nd",
    "2nd": "2nd",
    "2nd trimester": "2nd",
    "third": "3rd",
    "third trimester": "3rd",
    "3rd": "3rd",
    "3rd trimester": "3rd",
    "term": "Term",
    "premature": "Premature",
}

MISSING_VALUES = {"", "No", "N/A", "NA", "None", "nan", "Unknown"}

TABLE_COLUMNS = [
    "object_id",
    "gse_id",
    "title",
    "organism",
    "sample_size",
    "data_type",
    "library_strategy",
    "pregnancy_trimester",
    "pregnancy_complications_collected",
    "sample_country",
    "country",
    "pmid",
    "pmcid",
    "doi",
]

REPOSITORY_COUNT_COLUMNS = [
    "COALESCE(ec.evidence_count, 0) AS evidence_count",
    "COALESCE(sr.repair_count, 0) AS repair_count",
]


def _select_columns(columns):
    return [f"geo_metadata.{column} AS {column}" for column in columns]

EXPORT_COLUMNS = [
    "object_id",
    "gse_id",
    "title",
    "organism",
    "sample_size",
    "data_type",
    "library_strategy",
    "library_source",
    "library_selection",
    "instrument_model",
    "platform_id",
    "sra_study_id",
    "bioproject_id",
    "pregnancy_trimester",
    "birthweight_provided",
    "ga_delivery_provided",
    "ga_delivery_weeks",
    "ga_collection_provided",
    "ga_collection_weeks",
    "sex_provided",
    "race_ethnicity_provided",
    "genetic_ancestry_provided",
    "pregnancy_complications_collected",
    "pregnancy_complications_list",
    "fetal_complications_listed",
    "fetal_complications_list",
    "other_phenotypes",
    "hospital_center",
    "sample_country",
    "country",
    "pmid",
    "pmcid",
    "doi",
]


def get_db_connection():
    """Create a database connection with row factory for dict-like access."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.create_function("has_filter_value", 3, _has_filter_value)
    return conn


def _clean_value(value):
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text in MISSING_VALUES else text


def _facet_values(value, facet):
    """Return normalized atomic values from a GEO display field."""
    text = _clean_value(value)
    if not text:
        return []

    parts = re.split(r"\s*[,;]\s*", text) if facet in MULTI_VALUE_FACETS else [text]
    values = []
    for part in parts:
        part = part.strip()
        if not part:
            continue
        normalized_key = part.casefold()
        if facet == "country":
            part = COUNTRY_ALIASES.get(normalized_key, part)
        elif facet == "trimester":
            part = TRIMESTER_ALIASES.get(normalized_key, part)
        values.append(part)
    return values


def _has_filter_value(value, selected_value, facet):
    """SQLite callback used to match one normalized filter value."""
    selected = str(selected_value).casefold()
    return int(any(item.casefold() == selected for item in _facet_values(value, facet)))


def _table_columns(conn):
    cursor = conn.execute("PRAGMA table_info(geo_metadata)")
    return [row[1] for row in cursor.fetchall()]


def _table_exists(conn, table_name):
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        [table_name],
    ).fetchone()
    return row is not None


def _row_list(rows):
    return [dict(row) for row in rows]


def _value_counts(conn, column, limit=40):
    sql = f"""
        SELECT {column} AS value, COUNT(*) AS count
        FROM geo_metadata
        WHERE {column} IS NOT NULL
          AND TRIM(CAST({column} AS TEXT)) != ''
          AND TRIM(CAST({column} AS TEXT)) NOT IN ('No', 'N/A', 'NA', 'None', 'nan', 'Unknown')
        GROUP BY {column}
        ORDER BY count DESC, value ASC
        LIMIT ?
    """
    return [
        {"value": row["value"], "count": row["count"]}
        for row in conn.execute(sql, [limit]).fetchall()
    ]


def _facet_value_counts(conn, column, facet, limit=40, fallback_column=None):
    """Count normalized atomic values for a repository filter."""
    selected_column = column
    if fallback_column:
        selected_column = f"COALESCE(NULLIF({column}, ''), {fallback_column})"

    rows = conn.execute(
        f"""
        SELECT {selected_column} AS value
        FROM geo_metadata
        WHERE {selected_column} IS NOT NULL
        """
    ).fetchall()
    counts = Counter()
    for row in rows:
        counts.update(_facet_values(row["value"], facet))

    return [
        {"value": value, "count": count}
        for value, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:limit]
    ]


def _count_nonempty(conn, column):
    sql = f"""
        SELECT COUNT(*)
        FROM geo_metadata
        WHERE {column} IS NOT NULL
          AND TRIM(CAST({column} AS TEXT)) != ''
          AND TRIM(CAST({column} AS TEXT)) NOT IN ('No', 'N/A', 'NA', 'None', 'nan', 'Unknown')
    """
    return conn.execute(sql).fetchone()[0]


def _build_where(conn, query=None, filters=None):
    where_clauses = []
    params = []

    if query:
        columns = [col for col in _table_columns(conn) if col != "object_id"]
        search_clauses = [f"CAST(geo_metadata.{col} AS TEXT) LIKE ?" for col in columns]
        where_clauses.append(f"({' OR '.join(search_clauses)})")
        params.extend([f"%{query}%"] * len(columns))

    filters = filters or {}
    for filter_name, values in filters.items():
        if filter_name == "evidence":
            if "linked" in values and _table_exists(conn, "evidence_rows"):
                where_clauses.append(
                    "EXISTS (SELECT 1 FROM evidence_rows er WHERE er.gse_id = geo_metadata.gse_id)"
                )
            if "supplement_repair" in values and _table_exists(
                conn, "supplement_repair_rows"
            ):
                where_clauses.append(
                    "EXISTS (SELECT 1 FROM supplement_repair_rows sr WHERE sr.gse_id = geo_metadata.gse_id)"
                )
            continue

        column = FILTER_COLUMNS.get(filter_name)
        if not column or not values:
            continue
        source_column = f"geo_metadata.{column}"
        if filter_name == "country":
            source_column = "COALESCE(NULLIF(geo_metadata.sample_country, ''), geo_metadata.country)"
        matches = [f"has_filter_value({source_column}, ?, ?)" for _ in values]
        where_clauses.append(f"({' OR '.join(matches)})")
        for value in values:
            params.extend([value, filter_name])

    return (" AND ".join(where_clauses) if where_clauses else "1=1"), params


def get_atlas_summary():
    """Return atlas-level counts used on the landing page."""
    conn = get_db_connection()
    total = conn.execute("SELECT COUNT(*) FROM geo_metadata").fetchone()[0]
    sample_total = conn.execute(
        "SELECT COALESCE(SUM(sample_size), 0) FROM geo_metadata WHERE sample_size IS NOT NULL"
    ).fetchone()[0]
    publication_links = conn.execute(
        """
        SELECT COUNT(*)
        FROM geo_metadata
        WHERE (pmid IS NOT NULL AND CAST(pmid AS TEXT) != '')
           OR (pmcid IS NOT NULL AND TRIM(pmcid) != '')
           OR (doi IS NOT NULL AND TRIM(doi) != '')
        """
    ).fetchone()[0]
    if _table_exists(conn, "evidence_rows"):
        evidence_rows = conn.execute("SELECT COUNT(*) FROM evidence_rows").fetchone()[0]
        studies_with_evidence = conn.execute(
            "SELECT COUNT(DISTINCT gse_id) FROM evidence_rows"
        ).fetchone()[0]
    else:
        evidence_rows = 0
        studies_with_evidence = 0

    if _table_exists(conn, "supplement_repair_rows"):
        supplement_repair_rows = conn.execute(
            "SELECT COUNT(*) FROM supplement_repair_rows"
        ).fetchone()[0]
    else:
        supplement_repair_rows = 0

    summary = {
        "total": total,
        "sample_total": int(sample_total or 0),
        "publication_links": publication_links,
        "evidence_rows": evidence_rows,
        "studies_with_evidence": studies_with_evidence,
        "supplement_repair_rows": supplement_repair_rows,
        "organism_count": _count_nonempty(conn, "organism"),
        "distinct_organisms": len(_value_counts(conn, "organism", 100)),
        "distinct_data_types": len(_value_counts(conn, "data_type", 200)),
        "distinct_countries": len(_value_counts(conn, "sample_country", 200)),
        "top_organisms": _value_counts(conn, "organism", 6),
        "top_data_types": _value_counts(conn, "data_type", 8),
        "top_strategies": _value_counts(conn, "library_strategy", 8),
        "top_trimesters": _value_counts(conn, "pregnancy_trimester", 8),
        "top_countries": _value_counts(conn, "sample_country", 8),
    }
    conn.close()
    return summary


def get_featured_studies(limit=4):
    """Return real studies with linked workbook evidence for the landing page."""
    conn = get_db_connection()
    evidence_join = ""
    repair_join = ""
    evidence_columns = "0 AS evidence_count, 0 AS repair_count"
    order_by = "geo_metadata.object_id ASC"

    if _table_exists(conn, "evidence_rows"):
        evidence_join = """
            LEFT JOIN (
                SELECT gse_id, COUNT(*) AS evidence_count
                FROM evidence_rows
                GROUP BY gse_id
            ) ec ON ec.gse_id = geo_metadata.gse_id
        """
        evidence_columns = "COALESCE(ec.evidence_count, 0) AS evidence_count"
        order_by = "evidence_count DESC, geo_metadata.object_id ASC"

    if _table_exists(conn, "supplement_repair_rows"):
        repair_join = """
            LEFT JOIN (
                SELECT gse_id, COUNT(*) AS repair_count
                FROM supplement_repair_rows
                GROUP BY gse_id
            ) sr ON sr.gse_id = geo_metadata.gse_id
        """
        evidence_columns += ", COALESCE(sr.repair_count, 0) AS repair_count"
        order_by = "repair_count DESC, evidence_count DESC, geo_metadata.object_id ASC"
    elif _table_exists(conn, "evidence_rows"):
        evidence_columns += ", 0 AS repair_count"

    rows = conn.execute(
        f"""
        SELECT
            geo_metadata.object_id,
            geo_metadata.gse_id,
            geo_metadata.title,
            geo_metadata.organism,
            geo_metadata.sample_size,
            geo_metadata.data_type,
            geo_metadata.pregnancy_trimester,
            COALESCE(geo_metadata.sample_country, geo_metadata.country) AS sample_country,
            {evidence_columns}
        FROM geo_metadata
        {evidence_join}
        {repair_join}
        WHERE geo_metadata.gse_id IS NOT NULL
          AND TRIM(geo_metadata.gse_id) != ''
        ORDER BY {order_by}
        LIMIT ?
        """,
        [limit],
    ).fetchall()
    conn.close()
    return _row_list(rows)


def get_filter_options():
    """Return filter options with counts for the repository sidebar."""
    conn = get_db_connection()
    evidence_count = 0
    repair_count = 0
    if _table_exists(conn, "evidence_rows"):
        evidence_count = conn.execute(
            "SELECT COUNT(DISTINCT gse_id) FROM evidence_rows"
        ).fetchone()[0]
    if _table_exists(conn, "supplement_repair_rows"):
        repair_count = conn.execute(
            "SELECT COUNT(DISTINCT gse_id) FROM supplement_repair_rows"
        ).fetchone()[0]

    options = {
        "organisms": _value_counts(conn, "organism", 20),
        "data_types": _facet_value_counts(conn, "data_type", "data_type", 12),
        "data_types_more": _facet_value_counts(conn, "data_type", "data_type", 80)[12:],
        "library_strategies": _facet_value_counts(
            conn, "library_strategy", "library_strategy", 12
        ),
        "library_strategies_more": _facet_value_counts(
            conn, "library_strategy", "library_strategy", 80
        )[12:],
        "trimesters": _facet_value_counts(conn, "pregnancy_trimester", "trimester", 20),
        "countries": _facet_value_counts(
            conn, "sample_country", "country", 12, fallback_column="country"
        ),
        "countries_more": _facet_value_counts(
            conn, "sample_country", "country", 80, fallback_column="country"
        )[12:],
        "evidence": [
            {"value": "linked", "label": "Workbook evidence linked", "count": evidence_count},
            {
                "value": "supplement_repair",
                "label": "Supplement repair finding",
                "count": repair_count,
            },
        ],
    }
    conn.close()
    return options


def search_with_filters(query=None, filters=None, page=1, per_page=25):
    """Search and filter repository rows with pagination."""
    page = max(page, 1)
    conn = get_db_connection()
    offset = (page - 1) * per_page
    where_sql, params = _build_where(conn, query=query, filters=filters)
    selected_columns = ", ".join(_select_columns(TABLE_COLUMNS) + REPOSITORY_COUNT_COLUMNS)

    rows = conn.execute(
        f"""
        SELECT {selected_columns}
        FROM geo_metadata
        LEFT JOIN (
            SELECT gse_id, COUNT(*) AS evidence_count
            FROM evidence_rows
            GROUP BY gse_id
        ) ec ON ec.gse_id = geo_metadata.gse_id
        LEFT JOIN (
            SELECT gse_id, COUNT(*) AS repair_count
            FROM supplement_repair_rows
            GROUP BY gse_id
        ) sr ON sr.gse_id = geo_metadata.gse_id
        WHERE {where_sql}
        ORDER BY object_id
        LIMIT ? OFFSET ?
        """,
        params + [per_page, offset],
    ).fetchall()
    total = conn.execute(
        f"SELECT COUNT(*) FROM geo_metadata WHERE {where_sql}", params
    ).fetchone()[0]
    conn.close()

    return {
        "results": _row_list(rows),
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page,
    }


def export_rows(query=None, filters=None, limit=10000):
    """Return raw rows for CSV export using the current repository query."""
    conn = get_db_connection()
    where_sql, params = _build_where(conn, query=query, filters=filters)
    selected_columns = ", ".join(_select_columns(EXPORT_COLUMNS) + REPOSITORY_COUNT_COLUMNS)
    rows = conn.execute(
        f"""
        SELECT {selected_columns}
        FROM geo_metadata
        LEFT JOIN (
            SELECT gse_id, COUNT(*) AS evidence_count
            FROM evidence_rows
            GROUP BY gse_id
        ) ec ON ec.gse_id = geo_metadata.gse_id
        LEFT JOIN (
            SELECT gse_id, COUNT(*) AS repair_count
            FROM supplement_repair_rows
            GROUP BY gse_id
        ) sr ON sr.gse_id = geo_metadata.gse_id
        WHERE {where_sql}
        ORDER BY object_id
        LIMIT ?
        """,
        params + [limit],
    ).fetchall()
    conn.close()
    return _row_list(rows)


def get_all_entries(page=1, per_page=25):
    """Compatibility wrapper for API callers."""
    return search_with_filters(page=page, per_page=per_page)


def search_entries(query, page=1, per_page=25):
    """Compatibility wrapper for API callers."""
    return search_with_filters(query=query, page=page, per_page=per_page)


def get_entry_by_id(object_id):
    """Get a single entry by its object_id."""
    conn = get_db_connection()
    result = conn.execute(
        "SELECT * FROM geo_metadata WHERE object_id = ?", [object_id]
    ).fetchone()
    conn.close()
    return dict(result) if result else None


def get_entry_evidence(gse_id, limit=10):
    """Return source evidence notes linked to a GEO study."""
    if not gse_id:
        return []

    conn = get_db_connection()
    if not _table_exists(conn, "evidence_rows"):
        conn.close()
        return []

    rows = conn.execute(
        """
        SELECT
            question,
            answer,
            confidence,
            quote,
            source,
            evidence_source_type,
            reason,
            pmcid,
            doi
        FROM evidence_rows
        WHERE gse_id = ?
        ORDER BY
            CASE
                WHEN question = 'Main topic of the publication' THEN 1
                WHEN question = 'Sampling timing during pregnancy' THEN 2
                WHEN question LIKE 'Samples collected at single pregnancy time point%' THEN 3
                WHEN question LIKE 'Gestational Age%' THEN 4
                WHEN question LIKE 'Birthweight%' THEN 5
                WHEN question LIKE 'Samples from pregnancy complications%' THEN 6
                WHEN question LIKE 'Pregnancy complications%' THEN 7
                WHEN question LIKE 'Fetal complications%' THEN 8
                WHEN quote IS NOT NULL AND TRIM(quote) != '' THEN 9
                ELSE 10
            END,
            id
        LIMIT ?
        """,
        [gse_id, limit],
    ).fetchall()
    conn.close()
    return _row_list(rows)


def get_entry_evidence_summary(gse_id):
    """Return compact evidence counts for a GEO study."""
    if not gse_id:
        return {"evidence_count": 0, "repair_count": 0}

    conn = get_db_connection()
    evidence_count = 0
    repair_count = 0
    if _table_exists(conn, "evidence_rows"):
        evidence_count = conn.execute(
            "SELECT COUNT(*) FROM evidence_rows WHERE gse_id = ?", [gse_id]
        ).fetchone()[0]
    if _table_exists(conn, "supplement_repair_rows"):
        repair_count = conn.execute(
            "SELECT COUNT(*) FROM supplement_repair_rows WHERE gse_id = ?", [gse_id]
        ).fetchone()[0]
    conn.close()
    return {"evidence_count": evidence_count, "repair_count": repair_count}


def get_entry_supplement_repairs(gse_id, limit=5):
    """Return supplement repair findings linked to a GEO study."""
    if not gse_id:
        return []

    conn = get_db_connection()
    if not _table_exists(conn, "supplement_repair_rows"):
        conn.close()
        return []

    rows = conn.execute(
        """
        SELECT
            question,
            paper_answer,
            supplement_answer,
            merge_action,
            confidence,
            quote,
            source,
            reason
        FROM supplement_repair_rows
        WHERE gse_id = ?
        ORDER BY id
        LIMIT ?
        """,
        [gse_id, limit],
    ).fetchall()
    conn.close()
    return _row_list(rows)


def get_column_info():
    """Get column names and their display labels."""
    return {
        "object_id": "Object ID",
        "gse_id": "GEO Series ID",
        "data_type": "Data Type",
        "superseries": "SuperSeries",
        "sample_size": "Sample Size",
        "title": "Title",
        "organism": "Organism",
        "characteristics": "Characteristics",
        "extracted_molecule": "Extracted Molecule",
        "extraction_protocol": "Extraction Protocol",
        "library_strategy": "Library Strategy",
        "library_source": "Library Source",
        "library_selection": "Library Selection",
        "instrument_model": "Instrument Model",
        "assay_description": "Assay Description",
        "data_processing": "Data Processing",
        "platform_id": "Platform ID",
        "sra_study_id": "SRA Study ID",
        "bioproject_id": "BioProject ID",
        "file_types": "File Types",
        "submission_date": "Submission Date",
        "last_update_date": "Last Update Date",
        "organization_name": "Organization",
        "contact_name": "Contact Name",
        "email": "Email",
        "country": "Submitter Country",
        "pmid": "PubMed ID",
        "pmcid": "PMC ID",
        "doi": "DOI",
        "supervisor_name": "Supervisor/PI Name",
        "supervisor_email": "Supervisor/PI Email",
        "main_topic": "Main Topic",
        "pregnancy_trimester": "Pregnancy Trimester",
        "birthweight_provided": "Birthweight Provided",
        "ga_delivery_provided": "GA at Delivery Provided",
        "ga_delivery_weeks": "GA at Delivery (weeks)",
        "ga_collection_provided": "GA at Collection Provided",
        "ga_collection_weeks": "GA at Collection (weeks)",
        "sex_provided": "Sex of Offspring Provided",
        "parity_provided": "Parity Provided",
        "gravidity_provided": "Gravidity Provided",
        "offspring_number_provided": "Offspring Number Provided",
        "race_ethnicity_provided": "Race/Ethnicity Provided",
        "genetic_ancestry_provided": "Genetic Ancestry Provided",
        "maternal_height_provided": "Maternal Height Provided",
        "maternal_weight_provided": "Maternal Weight Provided",
        "paternal_height_provided": "Paternal Height Provided",
        "paternal_weight_provided": "Paternal Weight Provided",
        "maternal_age_provided": "Maternal Age Provided",
        "paternal_age_provided": "Paternal Age Provided",
        "pregnancy_complications_collected": "Pregnancy Complications Collected",
        "delivery_mode_provided": "Delivery Mode Provided",
        "pregnancy_complications_list": "Pregnancy Complications",
        "fetal_complications_listed": "Fetal Complications Listed",
        "fetal_complications_list": "Fetal Complications",
        "other_phenotypes": "Other Phenotypes",
        "hospital_center": "Hospital/Center",
        "sample_country": "Sample Collection Country",
    }


def clean_display(value, fallback="Not reported"):
    """Normalize display values for templates and API consumers."""
    return _clean_value(value) or fallback
