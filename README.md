# Placenta Research Database

A searchable atlas of placenta gene expression studies. Flask and SQLite power
local development; the public site is generated as static HTML and JavaScript
for GitHub Pages.

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Add the local database

Place `geo_metadata.db` in the project root. The database is intentionally
excluded from version control.

### 3. Run the Flask development server

```bash
python app.py
```

Then open http://localhost:5000 in your browser.

## Build the static site

```bash
python generate_static_site.py
```

The generated GitHub Pages site is written to `dist/`. The build contains
rendered study pages and a browser-side search index, but not the SQLite file.

## Project Structure

```
geo_website/
├── app.py              # Flask application
├── database.py         # Database query functions
├── generate_static_site.py # GitHub Pages generator
├── geo_metadata.db     # Local SQLite database (not committed)
├── requirements.txt    # Python dependencies
├── static/
│   ├── style.css       # Stylesheet
│   └── repository.js   # Static search and filtering
└── templates/
    ├── index.html      # Search page
    ├── entry.html      # Detail page
    └── repository.html # Filterable study index
```

## Data Fields

The database contains 57 fields including:
- GEO Series ID, Title, Organism, Sample Size
- Library Strategy, Instrument Model, Platform ID
- Pregnancy Trimester, Gestational Age, Complications
- Publication info (PMID, DOI, PMC)
- Contact and submission information

## License

MIT License
