"""
DuckDB Database Layer for Chromatin Data Manager
=================================================
Manages all persistent storage using DuckDB + Parquet.
"""

import duckdb
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
DB_PATH = DATA_DIR / "processed" / "chromatin_data.duckdb"


def get_connection():
    """Get a DuckDB connection, creating schema if needed."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    _init_schema(con)
    return con


def _migrate_columns(con):
    """Add new columns to existing tables if they don't exist (safe migration)."""
    def _add_col(table, col, col_type, default):
        try:
            con.execute(f"SELECT {col} FROM {table} LIMIT 0")
        except Exception:
            con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type} DEFAULT {default}")

    # cell_lines migrations
    _add_col('cell_lines', 'species', 'VARCHAR', "'Homo sapiens'")
    _add_col('cell_lines', 'genome_assembly', 'VARCHAR', "'GRCh38'")

    # chromatin_experiments migrations
    for col in ['species', 'genome_assembly', 'treatment', 'treatment_duration',
                'modification', 'condition', 'biosample_type', 'study', 'dataset_label']:
        _add_col('chromatin_experiments', col, 'VARCHAR', "''")


def _init_schema(con):
    """Create tables and views if they don't exist."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS cell_lines (
            cell_line_id INTEGER PRIMARY KEY,
            cell_line_name VARCHAR,
            cell_line_normalized VARCHAR,
            tissue_type VARCHAR DEFAULT 'Unknown',
            organism VARCHAR DEFAULT 'human',
            species VARCHAR DEFAULT 'Homo sapiens',
            genome_assembly VARCHAR DEFAULT 'GRCh38',
            biosample_type VARCHAR DEFAULT '',
            total_experiments INTEGER DEFAULT 0,
            has_ccre BOOLEAN DEFAULT FALSE,
            has_hic BOOLEAN DEFAULT FALSE,
            has_mcool BOOLEAN DEFAULT FALSE
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS chromatin_experiments (
            experiment_id INTEGER PRIMARY KEY,
            accession VARCHAR UNIQUE,
            cell_line_id INTEGER,
            cell_line_name VARCHAR,
            cell_line_raw VARCHAR,
            source VARCHAR,
            file_format VARCHAR,
            file_size_bytes BIGINT DEFAULT 0,
            file_size_gb DOUBLE DEFAULT 0,
            experiment_set VARCHAR,
            href VARCHAR,
            download_url VARCHAR,
            download_status VARCHAR DEFAULT 'pending',
            local_path VARCHAR,
            species VARCHAR DEFAULT 'Homo sapiens',
            genome_assembly VARCHAR DEFAULT '',
            treatment VARCHAR DEFAULT '',
            treatment_duration VARCHAR DEFAULT '',
            modification VARCHAR DEFAULT '',
            condition VARCHAR DEFAULT '',
            biosample_type VARCHAR DEFAULT '',
            study VARCHAR DEFAULT '',
            dataset_label VARCHAR DEFAULT '',
            date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            date_downloaded TIMESTAMP,
            FOREIGN KEY (cell_line_id) REFERENCES cell_lines(cell_line_id)
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS regulatory_annotations (
            annotation_id INTEGER PRIMARY KEY,
            accession VARCHAR UNIQUE,
            cell_line_id INTEGER,
            cell_line_name VARCHAR,
            annotation_type VARCHAR DEFAULT 'cCRE',
            file_format VARCHAR DEFAULT 'bed.gz',
            file_size_bytes BIGINT DEFAULT 0,
            file_size_mb DOUBLE DEFAULT 0,
            assembly VARCHAR DEFAULT 'GRCh38',
            href VARCHAR,
            download_url VARCHAR,
            download_status VARCHAR DEFAULT 'pending',
            local_path VARCHAR,
            output_type VARCHAR,
            date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (cell_line_id) REFERENCES cell_lines(cell_line_id)
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS download_queue (
            queue_id INTEGER PRIMARY KEY,
            file_type VARCHAR,
            file_accession VARCHAR,
            cell_line_name VARCHAR,
            download_url VARCHAR,
            file_size_bytes BIGINT DEFAULT 0,
            priority INTEGER DEFAULT 0,
            status VARCHAR DEFAULT 'queued',
            attempts INTEGER DEFAULT 0,
            last_attempt TIMESTAMP,
            error_message VARCHAR,
            local_path VARCHAR
        );
    """)

    # Migrate: add new columns if they don't exist yet
    _migrate_columns(con)

    con.execute("""
        CREATE OR REPLACE VIEW paired_datasets AS
        SELECT
            cl.cell_line_name,
            cl.cell_line_normalized,
            cl.tissue_type,
            cl.species,
            cl.genome_assembly,
            COUNT(DISTINCT ce.experiment_id) as chromatin_count,
            COUNT(DISTINCT ra.annotation_id) as ccre_count,
            COALESCE(SUM(DISTINCT ce.file_size_gb), 0) as total_chromatin_gb,
            COALESCE(MAX(ra.file_size_mb), 0) as total_ccre_mb,
            COALESCE(SUM(DISTINCT ce.file_size_gb), 0) + COALESCE(MAX(ra.file_size_mb), 0) / 1024 as total_size_gb
        FROM cell_lines cl
        LEFT JOIN chromatin_experiments ce ON cl.cell_line_id = ce.cell_line_id
        LEFT JOIN regulatory_annotations ra ON cl.cell_line_id = ra.cell_line_id
        WHERE cl.has_ccre = TRUE AND cl.has_hic = TRUE
        GROUP BY cl.cell_line_name, cl.cell_line_normalized, cl.tissue_type, cl.species, cl.genome_assembly;
    """)

    con.execute("""
        CREATE OR REPLACE VIEW stats_summary AS
        SELECT
            (SELECT COUNT(*) FROM cell_lines) as total_cell_lines,
            (SELECT COUNT(*) FROM cell_lines WHERE has_ccre AND has_hic) as paired_cell_lines,
            (SELECT COUNT(*) FROM chromatin_experiments) as total_chromatin,
            (SELECT COUNT(*) FROM regulatory_annotations) as total_ccre,
            (SELECT COALESCE(SUM(file_size_gb), 0) FROM chromatin_experiments) as total_chromatin_gb,
            (SELECT COALESCE(SUM(file_size_mb), 0) FROM regulatory_annotations) as total_ccre_mb,
            (SELECT COUNT(*) FROM chromatin_experiments WHERE download_status = 'downloaded') as downloaded_chromatin,
            (SELECT COUNT(*) FROM regulatory_annotations WHERE download_status = 'downloaded') as downloaded_ccre,
            (SELECT COUNT(DISTINCT species) FROM cell_lines) as species_count,
            (SELECT COUNT(*) FROM cell_lines WHERE species = 'Homo sapiens') as human_cell_lines,
            (SELECT COUNT(*) FROM cell_lines WHERE species = 'Mus musculus') as mouse_cell_lines,
            (SELECT COUNT(*) FROM chromatin_experiments WHERE treatment != '' AND treatment IS NOT NULL) as treated_experiments,
            (SELECT COUNT(*) FROM chromatin_experiments WHERE (treatment = '' OR treatment IS NULL)) as untreated_experiments;
    """)


def upsert_cell_line(con, name, normalized, tissue='Unknown', organism='human',
                     species='Homo sapiens', genome_assembly='', biosample_type=''):
    """Insert or update a cell line, return its ID."""
    existing = con.execute(
        "SELECT cell_line_id FROM cell_lines WHERE cell_line_normalized = ?",
        [normalized]
    ).fetchone()

    if existing:
        cl_id = existing[0]
        # Update species/assembly if we have better info
        if species and species != 'Homo sapiens':
            con.execute("UPDATE cell_lines SET species = ?, organism = ? WHERE cell_line_id = ?",
                        [species, organism, cl_id])
        if genome_assembly:
            con.execute("UPDATE cell_lines SET genome_assembly = ? WHERE cell_line_id = ?",
                        [genome_assembly, cl_id])
        if biosample_type:
            con.execute("UPDATE cell_lines SET biosample_type = ? WHERE cell_line_id = ?",
                        [biosample_type, cl_id])
        return cl_id

    max_id = con.execute("SELECT COALESCE(MAX(cell_line_id), 0) FROM cell_lines").fetchone()[0]
    new_id = max_id + 1
    con.execute(
        """INSERT INTO cell_lines (cell_line_id, cell_line_name, cell_line_normalized,
           tissue_type, organism, species, genome_assembly, biosample_type)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        [new_id, name, normalized, tissue, organism, species, genome_assembly, biosample_type]
    )
    return new_id


def upsert_chromatin_experiment(con, data):
    """Insert or update a chromatin experiment record."""
    existing = con.execute(
        "SELECT experiment_id FROM chromatin_experiments WHERE accession = ?",
        [data['accession']]
    ).fetchone()

    if existing:
        exp_id = existing[0]
        # Update metadata if we have new info
        updates = []
        vals = []
        for col in ['species', 'genome_assembly', 'treatment', 'treatment_duration',
                     'modification', 'condition', 'biosample_type', 'study', 'dataset_label']:
            v = data.get(col, '')
            if v:
                updates.append(f"{col} = ?")
                vals.append(v)
        if updates:
            vals.append(exp_id)
            con.execute(f"UPDATE chromatin_experiments SET {', '.join(updates)} WHERE experiment_id = ?", vals)
        return exp_id

    max_id = con.execute("SELECT COALESCE(MAX(experiment_id), 0) FROM chromatin_experiments").fetchone()[0]
    new_id = max_id + 1

    con.execute("""
        INSERT INTO chromatin_experiments
        (experiment_id, accession, cell_line_id, cell_line_name, cell_line_raw, source,
         file_format, file_size_bytes, file_size_gb, experiment_set, href, download_url,
         species, genome_assembly, treatment, treatment_duration, modification,
         condition, biosample_type, study, dataset_label)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        new_id, data['accession'], data.get('cell_line_id'),
        data.get('cell_line_name', ''), data.get('cell_line_raw', ''),
        data.get('source', '4DN'), data.get('file_format', 'mcool'),
        data.get('file_size_bytes', 0), data.get('file_size_bytes', 0) / (1024**3),
        data.get('experiment_set', ''), data.get('href', ''), data.get('download_url', ''),
        data.get('species', 'Homo sapiens'), data.get('genome_assembly', ''),
        data.get('treatment', ''), data.get('treatment_duration', ''),
        data.get('modification', ''), data.get('condition', ''),
        data.get('biosample_type', ''), data.get('study', ''), data.get('dataset_label', ''),
    ])
    return new_id


def upsert_regulatory_annotation(con, data):
    """Insert or update a regulatory annotation record."""
    existing = con.execute(
        "SELECT annotation_id FROM regulatory_annotations WHERE accession = ?",
        [data['accession']]
    ).fetchone()

    if existing:
        return existing[0]

    max_id = con.execute("SELECT COALESCE(MAX(annotation_id), 0) FROM regulatory_annotations").fetchone()[0]
    new_id = max_id + 1

    con.execute("""
        INSERT INTO regulatory_annotations
        (annotation_id, accession, cell_line_id, cell_line_name, file_format,
         file_size_bytes, file_size_mb, assembly, href, download_url, output_type)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        new_id, data['accession'], data.get('cell_line_id'),
        data.get('cell_line_name', ''), data.get('file_format', 'bed.gz'),
        data.get('file_size_bytes', 0), data.get('file_size_bytes', 0) / (1024**2),
        data.get('assembly', 'GRCh38'), data.get('href', ''),
        data.get('download_url', ''), data.get('output_type', '')
    ])
    return new_id


def update_cell_line_flags(con):
    """Update has_hic, has_ccre, total_experiments flags on cell_lines."""
    con.execute("""
        UPDATE cell_lines SET
            has_hic = (cell_line_id IN (SELECT DISTINCT cell_line_id FROM chromatin_experiments WHERE cell_line_id IS NOT NULL)),
            has_ccre = (cell_line_id IN (SELECT DISTINCT cell_line_id FROM regulatory_annotations WHERE cell_line_id IS NOT NULL)),
            has_mcool = (cell_line_id IN (SELECT DISTINCT cell_line_id FROM chromatin_experiments WHERE file_format = 'mcool' AND cell_line_id IS NOT NULL)),
            total_experiments = (SELECT COUNT(*) FROM chromatin_experiments ce WHERE ce.cell_line_id = cell_lines.cell_line_id)
    """)


def mark_downloaded(con, accession, local_path, file_type='chromatin'):
    """Mark a file as downloaded."""
    if file_type == 'chromatin':
        con.execute(
            "UPDATE chromatin_experiments SET download_status = 'downloaded', local_path = ?, date_downloaded = CURRENT_TIMESTAMP WHERE accession = ?",
            [str(local_path), accession]
        )
    else:
        con.execute(
            "UPDATE regulatory_annotations SET download_status = 'downloaded', local_path = ? WHERE accession = ?",
            [str(local_path), accession]
        )


def export_parquet(con, table_name, output_path):
    """Export a table to Parquet format."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY {table_name} TO '{output_path}' (FORMAT PARQUET)")
