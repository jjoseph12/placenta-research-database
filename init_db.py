"""
Initialize the SQLite database from the Excel file.
Adds object_id column as unique identifier for each entry.
"""

import sqlite3
import pandas as pd
from pathlib import Path

# Paths
EXCEL_PATH = Path(__file__).parent.parent / "R_Scripts" / "gse_metadata_full.xlsx"
DB_PATH = Path(__file__).parent / "geo_metadata.db"

# Column name mapping (Excel column -> SQLite column)
COLUMN_MAPPING = {
    'GEO Series ID (GSE___)': 'gse_id',
    'Data type': 'data_type',
    'SuperSeries, list GEO Series that are part of the SuperSeries': 'superseries',
    'Sample size (placenta)': 'sample_size',
    'Title': 'title',
    'Organism': 'organism',
    'Characteristics': 'characteristics',
    'Extracted molecule': 'extracted_molecule',
    'Extraction protocol': 'extraction_protocol',
    'Library Strategy': 'library_strategy',
    'Library source': 'library_source',
    'Library selection': 'library_selection',
    'Instrument model': 'instrument_model',
    'Assay description': 'assay_description',
    'Data processing': 'data_processing',
    'Platform ID (list)': 'platform_id',
    'SRA Study ID (raw data)': 'sra_study_id',
    'BioSample/BioProject ID': 'bioproject_id',
    'File types/resources provided (list)': 'file_types',
    'Submission date': 'submission_date',
    'Last update date': 'last_update_date',
    'Organization name': 'organization_name',
    'Contact name': 'contact_name',
    'E-mail(s)': 'email',
    'Country': 'country',
    'PMID': 'pmid',
    'PMCID': 'pmcid',
    'DOI': 'doi'
}

def init_database():
    """Initialize the database from Excel file."""
    print(f"Reading Excel file: {EXCEL_PATH}")
    df = pd.read_excel(EXCEL_PATH)
    
    print(f"Found {len(df)} entries with {len(df.columns)} columns")
    
    # Rename columns to SQLite-friendly names
    df = df.rename(columns=COLUMN_MAPPING)
    
    # Add object_id as the first column (1-indexed for user friendliness)
    df.insert(0, 'object_id', range(1, len(df) + 1))
    
    print(f"Added object_id column (1 to {len(df)})")
    
    # Remove existing database if it exists
    if DB_PATH.exists():
        DB_PATH.unlink()
        print(f"Removed existing database")
    
    # Create SQLite database
    conn = sqlite3.connect(DB_PATH)
    
    # Create table with proper schema
    create_table_sql = """
    CREATE TABLE geo_metadata (
        object_id INTEGER PRIMARY KEY,
        gse_id TEXT,
        data_type TEXT,
        superseries TEXT,
        sample_size INTEGER,
        title TEXT,
        organism TEXT,
        characteristics TEXT,
        extracted_molecule TEXT,
        extraction_protocol TEXT,
        library_strategy TEXT,
        library_source TEXT,
        library_selection TEXT,
        instrument_model TEXT,
        assay_description TEXT,
        data_processing TEXT,
        platform_id TEXT,
        sra_study_id TEXT,
        bioproject_id TEXT,
        file_types TEXT,
        submission_date TEXT,
        last_update_date TEXT,
        organization_name TEXT,
        contact_name TEXT,
        email TEXT,
        country TEXT,
        pmid REAL,
        pmcid TEXT,
        doi TEXT
    )
    """
    
    conn.execute(create_table_sql)
    print("Created geo_metadata table")
    
    # Insert data
    df.to_sql('geo_metadata', conn, if_exists='replace', index=False)
    
    # Create index for faster searching
    conn.execute("CREATE INDEX idx_gse_id ON geo_metadata(gse_id)")
    conn.execute("CREATE INDEX idx_organism ON geo_metadata(organism)")
    conn.execute("CREATE INDEX idx_title ON geo_metadata(title)")
    
    conn.commit()
    conn.close()
    
    print(f"Database created successfully at: {DB_PATH}")
    print(f"Total entries: {len(df)}")
    
    return len(df)

if __name__ == "__main__":
    init_database()
