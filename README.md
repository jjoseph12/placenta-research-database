# Placenta Research Database

A searchable web database for placenta gene expression studies, built with Flask and SQLite.

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Initialize the database (if needed)

```bash
python init_db.py
```

### 3. Run the server

```bash
python app.py
```

Then open http://localhost:5000 in your browser.

## Project Structure

```
geo_website/
├── app.py              # Flask application
├── database.py         # Database query functions
├── init_db.py          # Database initialization script
├── geo_metadata.db     # SQLite database (1,721 entries)
├── requirements.txt    # Python dependencies
├── static/
│   └── style.css       # Stylesheet
└── templates/
    ├── index.html      # Search page
    ├── entry.html      # Detail page
    └── 404.html        # Error page
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
