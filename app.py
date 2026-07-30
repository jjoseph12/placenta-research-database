"""
Flask application for the Placenta Study Atlas.

The app presents GEO placental studies with workbook evidence notes and
source records.
"""

import csv
import io

from flask import Flask, Response, jsonify, redirect, render_template, request, url_for

from database import (
    clean_display,
    export_rows,
    get_atlas_summary,
    get_column_info,
    get_entry_by_id,
    get_entry_evidence,
    get_entry_evidence_summary,
    get_entry_supplement_repairs,
    get_featured_studies,
    get_filter_options,
    search_entries,
    search_with_filters,
)

app = Flask(__name__)
app.jinja_env.filters["display"] = clean_display


def _selected_filters():
    return {
        "organism": request.args.getlist("organism"),
        "data_type": request.args.getlist("data_type"),
        "library_strategy": request.args.getlist("library_strategy"),
        "trimester": request.args.getlist("trimester"),
        "country": request.args.getlist("country"),
        "evidence": request.args.getlist("evidence"),
    }


def _active_filter_count(filters):
    return sum(len(values) for values in filters.values())


@app.route("/")
def index():
    """Atlas landing page with study-index entry points."""
    if request.args:
        return redirect(url_for("repository", **request.args))

    return render_template(
        "index.html",
        atlas=get_atlas_summary(),
        featured_studies=get_featured_studies(),
        filter_options=get_filter_options(),
        active_page="index",
    )


@app.route("/repository")
def repository():
    """Filterable study list."""
    query = request.args.get("q", "").strip()
    page = request.args.get("page", 1, type=int)
    filters = _selected_filters()
    active_filters = {key: value for key, value in filters.items() if value}

    data = search_with_filters(
        query=query,
        filters=active_filters,
        page=page,
        per_page=25,
    )

    return render_template(
        "repository.html",
        data=data,
        query=query,
        filter_options=get_filter_options(),
        selected_filters=filters,
        active_filter_count=_active_filter_count(filters),
        active_page="repository",
    )


@app.route("/repository/export.csv")
def repository_export():
    """Download the current study-index result as CSV."""
    query = request.args.get("q", "").strip()
    filters = {key: value for key, value in _selected_filters().items() if value}
    rows = export_rows(query=query, filters=filters)

    output = io.StringIO()
    if rows:
        fieldnames = list(rows[0].keys())
    else:
        fieldnames = [
            "object_id",
            "gse_id",
            "title",
            "organism",
            "sample_size",
            "data_type",
            "library_strategy",
            "pregnancy_trimester",
            "sample_country",
            "pmid",
            "pmcid",
            "doi",
        ]

    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

    response = Response(output.getvalue(), mimetype="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=placenta_study_index.csv"
    return response


@app.route("/entry/<int:object_id>")
def entry_detail(object_id):
    """Detail page for a single study."""
    entry = get_entry_by_id(object_id)
    if not entry:
        return render_template("404.html"), 404

    return render_template(
        "entry.html",
        entry=entry,
        evidence=get_entry_evidence(entry.get("gse_id")),
        evidence_summary=get_entry_evidence_summary(entry.get("gse_id")),
        supplement_repairs=get_entry_supplement_repairs(entry.get("gse_id")),
        column_info=get_column_info(),
        active_page="repository",
    )


@app.route("/api/search")
def api_search():
    """JSON API for search and filtered study-index requests."""
    query = request.args.get("q", "").strip()
    page = request.args.get("page", 1, type=int)
    filters = {key: value for key, value in _selected_filters().items() if value}

    if filters:
        data = search_with_filters(query=query, filters=filters, page=page)
    elif query:
        data = search_entries(query, page=page)
    else:
        data = search_with_filters(page=page)

    return jsonify(data)


if __name__ == "__main__":
    app.run(debug=True, port=5000)
