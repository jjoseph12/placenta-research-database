"""
Database utilities for Placenta Research Database.
Provides functions to query the SQLite database.
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "geo_metadata.db"

def get_db_connection():
    """Create a database connection with row factory for dict-like access."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def search_entries(query, page=1, per_page=20):
    """
    Search all text fields for the query string.
    Returns paginated results.
    """
    conn = get_db_connection()
    offset = (page - 1) * per_page
    
    # Get all column names from the database
    cursor = conn.execute("PRAGMA table_info(geo_metadata)")
    columns = [row[1] for row in cursor.fetchall()]
    
    # Build search query for all text columns (exclude object_id)
    text_columns = [col for col in columns if col != 'object_id']
    where_clauses = [f"{col} LIKE ?" for col in text_columns]
    where_sql = " OR ".join(where_clauses)
    
    search_term = f"%{query}%"
    params = [search_term] * len(text_columns)
    
    # Search query
    search_sql = f"""
    SELECT * FROM geo_metadata
    WHERE {where_sql}
    ORDER BY object_id
    LIMIT ? OFFSET ?
    """
    
    results = conn.execute(search_sql, params + [per_page, offset]).fetchall()
    
    # Get total count for pagination
    count_sql = f"SELECT COUNT(*) FROM geo_metadata WHERE {where_sql}"
    total = conn.execute(count_sql, params).fetchone()[0]
    conn.close()
    
    return {
        'results': [dict(row) for row in results],
        'total': total,
        'page': page,
        'per_page': per_page,
        'total_pages': (total + per_page - 1) // per_page
    }

def get_all_entries(page=1, per_page=20):
    """Get all entries with pagination."""
    conn = get_db_connection()
    offset = (page - 1) * per_page
    
    results = conn.execute(
        "SELECT * FROM geo_metadata ORDER BY object_id LIMIT ? OFFSET ?",
        [per_page, offset]
    ).fetchall()
    
    total = conn.execute("SELECT COUNT(*) FROM geo_metadata").fetchone()[0]
    conn.close()
    
    return {
        'results': [dict(row) for row in results],
        'total': total,
        'page': page,
        'per_page': per_page,
        'total_pages': (total + per_page - 1) // per_page
    }

def get_entry_by_id(object_id):
    """Get a single entry by its object_id."""
    conn = get_db_connection()
    result = conn.execute(
        "SELECT * FROM geo_metadata WHERE object_id = ?",
        [object_id]
    ).fetchone()
    conn.close()
    
    return dict(result) if result else None

def get_column_info():
    """Get column names and their display labels."""
    return {
        'object_id': 'Object ID',
        'gse_id': 'GEO Series ID',
        'data_type': 'Data Type',
        'superseries': 'SuperSeries',
        'sample_size': 'Sample Size',
        'title': 'Title',
        'organism': 'Organism',
        'characteristics': 'Characteristics',
        'extracted_molecule': 'Extracted Molecule',
        'extraction_protocol': 'Extraction Protocol',
        'library_strategy': 'Library Strategy',
        'library_source': 'Library Source',
        'library_selection': 'Library Selection',
        'instrument_model': 'Instrument Model',
        'assay_description': 'Assay Description',
        'data_processing': 'Data Processing',
        'platform_id': 'Platform ID',
        'sra_study_id': 'SRA Study ID',
        'bioproject_id': 'BioProject ID',
        'file_types': 'File Types',
        'submission_date': 'Submission Date',
        'last_update_date': 'Last Update Date',
        'organization_name': 'Organization',
        'contact_name': 'Contact Name',
        'email': 'Email',
        'country': 'Country',
        'pmid': 'PubMed ID',
        'pmcid': 'PMC ID',
        'doi': 'DOI',
        'supervisor_name': 'Supervisor/PI Name',
        'supervisor_email': 'Supervisor/PI Email',
        'main_topic': 'Main Topic',
        'pregnancy_trimester': 'Pregnancy Trimester',
        'birthweight_provided': 'Birthweight Provided',
        'ga_delivery_provided': 'GA at Delivery Provided',
        'ga_delivery_weeks': 'GA at Delivery (weeks)',
        'ga_collection_provided': 'GA at Collection Provided',
        'ga_collection_weeks': 'GA at Collection (weeks)',
        'sex_provided': 'Sex of Offspring Provided',
        'parity_provided': 'Parity Provided',
        'gravidity_provided': 'Gravidity Provided',
        'offspring_number_provided': 'Offspring Number Provided',
        'race_ethnicity_provided': 'Race/Ethnicity Provided',
        'genetic_ancestry_provided': 'Genetic Ancestry Provided',
        'maternal_height_provided': 'Maternal Height Provided',
        'maternal_weight_provided': 'Maternal Weight Provided',
        'paternal_height_provided': 'Paternal Height Provided',
        'paternal_weight_provided': 'Paternal Weight Provided',
        'maternal_age_provided': 'Maternal Age Provided',
        'paternal_age_provided': 'Paternal Age Provided',
        'pregnancy_complications_collected': 'Pregnancy Complications Collected',
        'delivery_mode_provided': 'Delivery Mode Provided',
        'pregnancy_complications_list': 'Pregnancy Complications',
        'fetal_complications_listed': 'Fetal Complications Listed',
        'fetal_complications_list': 'Fetal Complications',
        'other_phenotypes': 'Other Phenotypes',
        'hospital_center': 'Hospital/Center',
        'sample_country': 'Sample Collection Country'
    }

def get_filter_options():
    """Get unique values for filter dropdowns."""
    conn = get_db_connection()
    
    # Get unique organisms
    organisms = conn.execute(
        "SELECT DISTINCT organism FROM geo_metadata WHERE organism IS NOT NULL AND organism != '' ORDER BY organism"
    ).fetchall()
    
    # Get unique data types
    data_types = conn.execute(
        "SELECT DISTINCT data_type FROM geo_metadata WHERE data_type IS NOT NULL AND data_type != '' ORDER BY data_type"
    ).fetchall()
    
    # Get unique library strategies
    strategies = conn.execute(
        "SELECT DISTINCT library_strategy FROM geo_metadata WHERE library_strategy IS NOT NULL AND library_strategy != '' ORDER BY library_strategy"
    ).fetchall()
    
    conn.close()
    
    return {
        'organisms': [row[0] for row in organisms],
        'data_types': [row[0] for row in data_types],
        'library_strategies': [row[0] for row in strategies]
    }

def search_with_filters(query=None, filters=None, page=1, per_page=20):
    """Search with optional filters."""
    conn = get_db_connection()
    offset = (page - 1) * per_page
    
    where_clauses = []
    params = []
    
    # Add text search if query provided
    if query:
        cursor = conn.execute("PRAGMA table_info(geo_metadata)")
        columns = [row[1] for row in cursor.fetchall()]
        text_columns = [col for col in columns if col != 'object_id']
        search_clauses = [f"{col} LIKE ?" for col in text_columns]
        where_clauses.append(f"({' OR '.join(search_clauses)})")
        params.extend([f"%{query}%"] * len(text_columns))
    
    # Add filters
    if filters:
        if 'organisms' in filters and filters['organisms']:
            placeholders = ','.join(['?' for _ in filters['organisms']])
            where_clauses.append(f"organism IN ({placeholders})")
            params.extend(filters['organisms'])
        if 'data_types' in filters and filters['data_types']:
            placeholders = ','.join(['?' for _ in filters['data_types']])
            where_clauses.append(f"data_type IN ({placeholders})")
            params.extend(filters['data_types'])
        if 'library_strategies' in filters and filters['library_strategies']:
            placeholders = ','.join(['?' for _ in filters['library_strategies']])
            where_clauses.append(f"library_strategy IN ({placeholders})")
            params.extend(filters['library_strategies'])
    
    # Build query
    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
    
    search_sql = f"""
    SELECT * FROM geo_metadata
    WHERE {where_sql}
    ORDER BY object_id
    LIMIT ? OFFSET ?
    """
    
    results = conn.execute(search_sql, params + [per_page, offset]).fetchall()
    
    # Get total count
    count_sql = f"SELECT COUNT(*) FROM geo_metadata WHERE {where_sql}"
    total = conn.execute(count_sql, params).fetchone()[0]
    conn.close()
    
    return {
        'results': [dict(row) for row in results],
        'total': total,
        'page': page,
        'per_page': per_page,
        'total_pages': (total + per_page - 1) // per_page
    }
