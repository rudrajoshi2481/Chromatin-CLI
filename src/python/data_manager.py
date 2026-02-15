#!/usr/bin/env python3
"""
Chromatin Data Manager - Core Engine
=====================================
Fetches chromatin interaction files (.mcool) from 4DN and
regulatory annotations (cCRE BED) from ENCODE.

Uses DuckDB for persistent storage, supports parallel downloads,
and exposes a JSON IPC interface for the Ink CLI frontend.
"""

import os
import sys
import json
import time
import gzip
import hashlib
import requests
import pandas as pd
import duckdb
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# Load environment
load_dotenv()

FOURDN_ACCESS_ID = os.getenv("FOURDN_ACCESS_ID", "")
FOURDN_SECRET_KEY = os.getenv("FOURDN_SECRET_KEY", "")
FOURDN_API = "https://data.4dnucleome.org"
ENCODE_API = "https://www.encodeproject.org"

DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
MCOOL_DIR = DATA_DIR / "downloads" / "mcool"
CCRE_DIR = DATA_DIR / "downloads" / "ccre"
CACHE_DIR = DATA_DIR / "cache"
REPORT_DIR = DATA_DIR / "reports"
RAW_DIR = DATA_DIR / "raw"
MAX_PARALLEL = int(os.getenv("MAX_PARALLEL_DOWNLOADS", "3"))
CACHE_TTL = float(os.getenv("CACHE_TTL_HOURS", "24"))


def _sizeof_fmt(num_bytes):
    """Human-readable file size."""
    if num_bytes is None or num_bytes == 0:
        return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} PB"


# ---------------------------------------------------------------------------
# Cell line normalization
# ---------------------------------------------------------------------------
_KNOWN_CELL_LINES = {
    "gm12878": "GM12878",
    "k562": "K562",
    "hct116": "HCT116",
    "hela-s3": "HeLa-S3",
    "hepg2": "HepG2",
    "imr-90": "IMR-90",
    "h9": "H9",
    "h1-hesc": "H1",
    "hap-1": "HAP-1",
    "kbm-7": "KBM-7",
    "ch12.lx": "CH12.LX",
    "es-e14": "ES-E14",
    "es-e14tg2a": "ES-E14",
    "hff-htert": "foreskin fibroblast",
    "huvec cell": "HUVEC",
    "wtc-11": "WTC11",
    "mcf-7": "MCF-7",
    "a549": "A549",
    "panc1": "Panc1",
    "pc-3": "PC-3",
    "lncap": "LNCaP clone FGC",
    "caco-2": "Caco-2",
    "cc-2551": "IMR-90",
}

_TISSUE_MAP = {
    "GM12878": "B-lymphocyte",
    "K562": "Leukemia (CML)",
    "HCT116": "Colon carcinoma",
    "HeLa-S3": "Cervical carcinoma",
    "HepG2": "Hepatocellular carcinoma",
    "IMR-90": "Fetal lung fibroblast",
    "H9": "Embryonic stem cell",
    "H1": "Embryonic stem cell",
    "HAP-1": "Chronic myelogenous leukemia",
    "KBM-7": "Chronic myelogenous leukemia",
    "CH12.LX": "B-cell lymphoma (mouse)",
    "ES-E14": "Embryonic stem cell (mouse)",
    "HUVEC": "Umbilical vein endothelial",
    "WTC11": "iPSC",
    "MCF-7": "Breast carcinoma",
    "A549": "Lung carcinoma",
    "Panc1": "Pancreatic carcinoma",
    "PC-3": "Prostate carcinoma",
    "LNCaP clone FGC": "Prostate carcinoma",
    "Caco-2": "Colorectal carcinoma",
    "foreskin fibroblast": "Foreskin fibroblast",
}


# Species detection from cell line names and 4DN metadata
_SPECIES_MAP = {
    'human': 'Homo sapiens',
    'mouse': 'Mus musculus',
    'drosophila': 'Drosophila melanogaster',
    'zebrafish': 'Danio rerio',
    'c. elegans': 'Caenorhabditis elegans',
    'rat': 'Rattus norvegicus',
}

_MOUSE_INDICATORS = [
    'mouse', 'mesc', 'es-e14', 'ch12', 'g1e-er4', 'g1e', 'mef',
    'cerebellar granule neuron', 'olfactory receptor cell',
    'thymocyte', 'treg', 'tcon', 'foxp3', 'pnd ', 'postnatal',
    'inner cell mass', 'embryo', '2-cell', '8-cell', 'morula',
    'blastocyst', 'trophoblast', 'cerebellum', 'cortex',
]

_HUMAN_INDICATORS = [
    'gm12878', 'k562', 'hct116', 'hela', 'hepg2', 'imr-90', 'imr90',
    'h1-hesc', 'h1 ', 'h9 ', 'hap-1', 'kbm-7', 'wtc11', 'wtc-11',
    'huvec', 'mcf-7', 'a549', 'panc1', 'pc-3', 'lncap', 'caco-2',
    'foreskin fibroblast', 'hff', 'rptel', 'htert', 'pdx1',
    'cardiac muscle cell', 'heart left',
]

# Genome assembly mapping
_ASSEMBLY_MAP = {
    'Homo sapiens': 'GRCh38',
    'Mus musculus': 'mm10',
    'Drosophila melanogaster': 'dm6',
    'Danio rerio': 'danRer11',
}


def detect_species(cell_line_raw, organism_name=None):
    """Detect species from cell line name and/or 4DN organism metadata."""
    if organism_name:
        org_lower = organism_name.lower()
        for key, species in _SPECIES_MAP.items():
            if key in org_lower:
                return species

    cl_lower = (cell_line_raw or '').lower()
    if any(ind in cl_lower for ind in _HUMAN_INDICATORS):
        return 'Homo sapiens'
    if any(ind in cl_lower for ind in _MOUSE_INDICATORS):
        return 'Mus musculus'
    if 'zebrafish' in cl_lower:
        return 'Danio rerio'
    if 'drosophila' in cl_lower:
        return 'Drosophila melanogaster'

    return 'Homo sapiens'  # default assumption


def get_genome_assembly(species):
    """Get default genome assembly for a species."""
    return _ASSEMBLY_MAP.get(species, 'GRCh38')


def normalize_cell_line(raw_name):
    """Normalize a verbose 4DN cell line name to a base name ENCODE recognizes."""
    if not raw_name or raw_name == 'Unknown':
        return raw_name

    name = raw_name.strip()

    # Try exact match first (case-insensitive)
    if name.lower() in _KNOWN_CELL_LINES:
        return _KNOWN_CELL_LINES[name.lower()]

    # Try prefix match: check if the name starts with a known cell line
    name_lower = name.lower()
    for key, canonical in sorted(_KNOWN_CELL_LINES.items(), key=lambda x: -len(x[0])):
        if name_lower.startswith(key):
            rest = name_lower[len(key):]
            if not rest or rest[0] in (' ', '(', ',', '-', '_', '.', '/'):
                return canonical

    # Try splitting on common delimiters
    for sep in [' with ', ' differentiated to ', ' - clone ', ' (']:
        if sep in name:
            base = name.split(sep)[0].strip()
            base_lower = base.lower()
            if base_lower in _KNOWN_CELL_LINES:
                return _KNOWN_CELL_LINES[base_lower]

    return raw_name


def get_tissue(cell_line_normalized):
    """Get tissue type for a normalized cell line name."""
    return _TISSUE_MAP.get(cell_line_normalized, "Unknown")


# ---------------------------------------------------------------------------
# Data Manager Class
# ---------------------------------------------------------------------------
class DataManager:
    def __init__(self, verbose=False, use_cache=True, db_con=None):
        self.verbose = verbose
        self.use_cache = use_cache
        self.session_4dn = self._setup_4dn_session()
        self.session_encode = requests.Session()
        self.session_encode.headers.update({'Accept': 'application/json'})
        self._ccre_cache = {}

        # DuckDB connection
        if db_con:
            self.con = db_con
        else:
            from . import db
            self.con = db.get_connection()

        # Create directories
        for d in [MCOOL_DIR, CCRE_DIR, CACHE_DIR, REPORT_DIR, RAW_DIR]:
            d.mkdir(parents=True, exist_ok=True)

    def _setup_4dn_session(self):
        """Setup 4DN session with authentication."""
        session = requests.Session()
        if FOURDN_ACCESS_ID and FOURDN_SECRET_KEY:
            session.auth = (FOURDN_ACCESS_ID, FOURDN_SECRET_KEY)
        return session

    def _log(self, msg):
        if self.verbose:
            print(f"[INFO] {msg}", file=sys.stderr)

    # ------------------------------------------------------------------
    # 4DN .mcool fetching
    # ------------------------------------------------------------------
    def fetch_4dn_mcool_files(self, cell_line_filter=None, force_refresh=False):
        """Fetch all .mcool files from 4DN and store in DuckDB."""
        cache_file = CACHE_DIR / "4dn_mcool_inventory.json"

        # Check cache
        if self.use_cache and not force_refresh and cache_file.exists():
            age_hours = (time.time() - cache_file.stat().st_mtime) / 3600
            if age_hours < CACHE_TTL:
                self._log(f"Using cached 4DN data (age: {age_hours:.1f}h)")
                with open(cache_file) as f:
                    all_files = json.load(f)
                self._store_mcool_in_db(all_files)
                if cell_line_filter:
                    return [f for f in all_files if cell_line_filter.lower() in f.get('cell_line', '').lower()]
                return all_files

        self._log("Fetching 4DN ExperimentSets (Hi-C)...")

        all_files = []
        from_pos = 0
        batch_size = 100

        while True:
            params = {
                "type": "ExperimentSetReplicate",
                "experiments_in_set.experiment_type.display_title": "in situ Hi-C",
                "status": "released",
                "format": "json",
                "limit": batch_size,
                "from": from_pos
            }

            try:
                r = self.session_4dn.get(f"{FOURDN_API}/search/", params=params, timeout=60)
                r.raise_for_status()
                data = r.json()

                exps = data.get('@graph', [])
                if not exps:
                    break

                total = data.get('total', 0)
                self._log(f"  4DN batch {from_pos}-{from_pos+len(exps)} / {total}")

                for exp in exps:
                    cell_line = exp.get('biosource_summary') or ''
                    if not cell_line:
                        eis = exp.get('experiments_in_set', [])
                        if eis and isinstance(eis[0], dict):
                            bs = eis[0].get('biosample', {})
                            if isinstance(bs, dict):
                                cell_line = bs.get('biosource_summary', '')
                            elif isinstance(bs, list) and bs and isinstance(bs[0], dict):
                                cell_line = bs[0].get('biosource_summary', '')
                    if not cell_line:
                        cell_line = 'Unknown'

                    for pf in exp.get('processed_files', []):
                        if not isinstance(pf, dict):
                            continue

                        fformat = pf.get('file_format', {})
                        if isinstance(fformat, dict):
                            fformat = fformat.get('display_title', '')

                        is_mcool = (
                            'mcool' in str(fformat).lower() or
                            str(pf.get('href', '')).endswith('.mcool')
                        )

                        if is_mcool:
                            # Extract rich metadata from experiment set
                            organism_name = ''
                            treatment_summary = ''
                            modification_summary = ''
                            biosample_type = ''
                            tissue_from_api = ''

                            eis = exp.get('experiments_in_set', [])
                            if eis and isinstance(eis[0], dict):
                                bs = eis[0].get('biosample', {})
                                if isinstance(bs, dict):
                                    treatment_summary = bs.get('treatments_summary', '') or ''
                                    if treatment_summary.lower() in ('none', 'no treatment', ''):
                                        treatment_summary = ''
                                    modification_summary = bs.get('modifications_summary', '') or ''
                                    if modification_summary.lower() in ('none', ''):
                                        modification_summary = ''
                                    biosample_type = bs.get('biosample_type', '') or ''
                                    # Get organism from biosource
                                    bsrc = bs.get('biosource', [])
                                    if isinstance(bsrc, list) and bsrc:
                                        src = bsrc[0] if isinstance(bsrc[0], dict) else {}
                                        org = src.get('organism', {})
                                        if isinstance(org, dict):
                                            organism_name = org.get('name', '') or org.get('scientific_name', '')
                                        tissue_obj = src.get('tissue', {})
                                        if isinstance(tissue_obj, dict):
                                            tissue_from_api = tissue_obj.get('term_name', '')
                                    elif isinstance(bsrc, dict):
                                        org = bsrc.get('organism', {})
                                        if isinstance(org, dict):
                                            organism_name = org.get('name', '') or org.get('scientific_name', '')

                            species = detect_species(cell_line, organism_name)
                            assembly = get_genome_assembly(species)
                            condition = exp.get('condition', '') or ''
                            study = exp.get('study', '') or ''
                            dataset_label = exp.get('dataset_label', '') or ''

                            all_files.append({
                                'source': '4DN',
                                'accession': pf.get('accession', ''),
                                'cell_line': cell_line,
                                'cell_line_normalized': normalize_cell_line(cell_line),
                                'file_size': pf.get('file_size', 0),
                                'file_format': 'mcool',
                                'href': pf.get('href', ''),
                                'experiment_set': exp.get('accession', ''),
                                'download_url': f"{FOURDN_API}{pf.get('href', '')}",
                                'species': species,
                                'genome_assembly': assembly,
                                'treatment': treatment_summary,
                                'modification': modification_summary,
                                'condition': condition,
                                'biosample_type': biosample_type,
                                'study': study,
                                'dataset_label': dataset_label,
                                'tissue_from_api': tissue_from_api,
                            })

                from_pos += len(exps)
                if from_pos >= total:
                    break

                time.sleep(0.5)

            except Exception as e:
                self._log(f"4DN fetch failed at offset {from_pos}: {e}")
                break

        # Cache to disk
        with open(cache_file, 'w') as f:
            json.dump(all_files, f, indent=2)

        # Also save as Parquet
        if all_files:
            df = pd.DataFrame(all_files)
            parquet_path = RAW_DIR / "4dn_experiments.parquet"
            df.to_parquet(str(parquet_path), index=False)

        # Store in DuckDB
        self._store_mcool_in_db(all_files)

        self._log(f"Found {len(all_files)} .mcool files from 4DN")

        if cell_line_filter:
            return [f for f in all_files if cell_line_filter.lower() in f.get('cell_line', '').lower()]
        return all_files

    def _store_mcool_in_db(self, files):
        """Store fetched mcool files into DuckDB."""
        from . import db
        for f in files:
            if not f.get('accession'):
                continue
            norm = f.get('cell_line_normalized') or normalize_cell_line(f.get('cell_line', 'Unknown'))
            tissue = f.get('tissue_from_api') or get_tissue(norm)
            species = f.get('species', 'Homo sapiens')
            assembly = f.get('genome_assembly', get_genome_assembly(species))
            biosample_type = f.get('biosample_type', '')

            cl_id = db.upsert_cell_line(
                self.con, f.get('cell_line', 'Unknown'), norm, tissue,
                organism=species.split()[-1].lower() if species else 'human',
                species=species, genome_assembly=assembly,
                biosample_type=biosample_type,
            )
            db.upsert_chromatin_experiment(self.con, {
                'accession': f['accession'],
                'cell_line_id': cl_id,
                'cell_line_name': norm,
                'cell_line_raw': f.get('cell_line', ''),
                'source': f.get('source', '4DN'),
                'file_format': f.get('file_format', 'mcool'),
                'file_size_bytes': f.get('file_size', 0),
                'experiment_set': f.get('experiment_set', ''),
                'href': f.get('href', ''),
                'download_url': f.get('download_url', ''),
                'species': species,
                'genome_assembly': assembly,
                'treatment': f.get('treatment', ''),
                'treatment_duration': f.get('treatment_duration', ''),
                'modification': f.get('modification', ''),
                'condition': f.get('condition', ''),
                'biosample_type': biosample_type,
                'study': f.get('study', ''),
                'dataset_label': f.get('dataset_label', ''),
            })
        db.update_cell_line_flags(self.con)

    # ------------------------------------------------------------------
    # ENCODE cCRE BED fetching
    # ------------------------------------------------------------------
    def fetch_ccre_for_cell_line(self, cell_line):
        """Fetch the actual cCRE BED file info from ENCODE for a specific cell line."""
        cell_norm = cell_line.lower().strip()

        if cell_norm in self._ccre_cache:
            return self._ccre_cache[cell_norm]

        # Check disk cache
        cache_file = CACHE_DIR / "ccre_by_cell.json"
        if self.use_cache and cache_file.exists():
            with open(cache_file) as f:
                disk_cache = json.load(f)
            if cell_norm in disk_cache:
                self._ccre_cache[cell_norm] = disk_cache[cell_norm]
                return disk_cache[cell_norm]

        self._log(f"  Querying ENCODE cCRE for: {cell_line}")
        result = None

        # Strategy 1: Search annotations
        try:
            params = {
                "type": "Annotation",
                "annotation_type": "candidate Cis-Regulatory Elements",
                "biosample_ontology.term_name": cell_line,
                "assembly": "GRCh38",
                "status": "released",
                "format": "json",
                "limit": "5",
            }
            r = self.session_encode.get(f"{ENCODE_API}/search/", params=params, timeout=60)
            if r.status_code == 200:
                data = r.json()
                for ann in data.get('@graph', []):
                    ann_id = ann.get('@id', '')
                    detail_r = self.session_encode.get(
                        f"{ENCODE_API}{ann_id}",
                        params={"format": "json"},
                        timeout=60
                    )
                    if detail_r.status_code != 200:
                        continue

                    detail = detail_r.json()
                    for file_ref in detail.get('files', []):
                        file_id = file_ref if isinstance(file_ref, str) else file_ref.get('@id', '')
                        if not file_id:
                            continue

                        file_r = self.session_encode.get(
                            f"{ENCODE_API}{file_id}",
                            params={"format": "json"},
                            timeout=60
                        )
                        if file_r.status_code != 200:
                            continue

                        fdata = file_r.json()
                        fformat = fdata.get('file_format', '')
                        output_type = fdata.get('output_type', '')
                        status = fdata.get('status', '')

                        if fformat == 'bed' and status == 'released' and 'cis' in output_type.lower():
                            result = {
                                'accession': fdata.get('accession'),
                                'href': fdata.get('href', ''),
                                'download_url': f"{ENCODE_API}{fdata.get('href', '')}",
                                'file_size': fdata.get('file_size', 0),
                                'file_format': 'bed.gz',
                                'assembly': fdata.get('assembly', 'GRCh38'),
                                'output_type': output_type,
                                'cell_line': cell_line,
                            }
                            break
                    if result:
                        break
        except Exception as e:
            self._log(f"  Error querying cCRE for {cell_line}: {e}")

        # Strategy 2: Direct file search
        if result is None:
            try:
                params2 = {
                    "type": "File",
                    "file_format": "bed",
                    "output_type": "candidate Cis-Regulatory Elements",
                    "biosample_ontology.term_name": cell_line,
                    "assembly": "GRCh38",
                    "status": "released",
                    "format": "json",
                    "limit": "3",
                }
                r2 = self.session_encode.get(f"{ENCODE_API}/search/", params=params2, timeout=60)
                if r2.status_code == 200:
                    data2 = r2.json()
                    files2 = data2.get('@graph', [])
                    if files2:
                        fdata = files2[0]
                        result = {
                            'accession': fdata.get('accession'),
                            'href': fdata.get('href', ''),
                            'download_url': f"{ENCODE_API}{fdata.get('href', '')}",
                            'file_size': fdata.get('file_size', 0),
                            'file_format': 'bed.gz',
                            'assembly': fdata.get('assembly', 'GRCh38'),
                            'output_type': fdata.get('output_type', ''),
                            'cell_line': cell_line,
                        }
            except Exception as e:
                self._log(f"  Fallback cCRE search failed for {cell_line}: {e}")

        # Cache result
        self._ccre_cache[cell_norm] = result

        # Persist to disk cache
        disk_cache = {}
        if cache_file.exists():
            try:
                with open(cache_file) as f:
                    disk_cache = json.load(f)
            except json.JSONDecodeError:
                disk_cache = {}
        disk_cache[cell_norm] = result
        with open(cache_file, 'w') as f:
            json.dump(disk_cache, f, indent=2)

        # Store in DuckDB
        if result and result.get('accession'):
            from . import db
            norm = normalize_cell_line(cell_line)
            tissue = get_tissue(norm)
            cl_id = db.upsert_cell_line(self.con, cell_line, norm, tissue)
            db.upsert_regulatory_annotation(self.con, {
                'accession': result['accession'],
                'cell_line_id': cl_id,
                'cell_line_name': norm,
                'file_format': result.get('file_format', 'bed.gz'),
                'file_size_bytes': result.get('file_size', 0),
                'assembly': result.get('assembly', 'GRCh38'),
                'href': result.get('href', ''),
                'download_url': result.get('download_url', ''),
                'output_type': result.get('output_type', ''),
            })
            db.update_cell_line_flags(self.con)

        return result

    def fetch_all_ccre(self, cell_lines):
        """Fetch cCRE info for a list of cell lines."""
        results = {}
        unique = sorted(set(cell_lines))
        for i, cell in enumerate(unique, 1):
            self._log(f"  [{i}/{len(unique)}] Fetching cCRE for: {cell}")
            info = self.fetch_ccre_for_cell_line(cell)
            if info:
                results[cell] = info
            time.sleep(0.3)
        return results

    # ------------------------------------------------------------------
    # Paired datasets
    # ------------------------------------------------------------------
    def find_paired_datasets(self, cell_line_filter=None, one_per_cell=False,
                             min_replicates=None, max_replicates=None,
                             min_size_gb=None, max_size_gb=None,
                             tissues=None):
        """Find truly paired datasets: .mcool files with matching cCRE BED."""
        mcool_files = self.fetch_4dn_mcool_files(cell_line_filter=cell_line_filter)
        mcool_files = [f for f in mcool_files if f.get('cell_line', '').lower().strip() != 'unknown']

        if not mcool_files:
            return []

        # Normalize
        for mf in mcool_files:
            mf['cell_line_raw'] = mf.get('cell_line', '')
            mf['cell_line_normalized'] = mf.get('cell_line_normalized') or normalize_cell_line(mf['cell_line'])

        unique_norm_cells = sorted(set(f['cell_line_normalized'] for f in mcool_files))
        ccre_map = self.fetch_all_ccre(unique_norm_cells)

        # Build paired list
        paired = []
        for mf in mcool_files:
            norm_cell = mf['cell_line_normalized']
            ccre = ccre_map.get(norm_cell)
            if ccre is None:
                continue

            paired.append({
                'cell_line': norm_cell,
                'cell_line_raw': mf['cell_line_raw'],
                'tissue': get_tissue(norm_cell),
                'mcool_accession': mf.get('accession', ''),
                'mcool_size': mf.get('file_size', 0),
                'mcool_size_gb': mf.get('file_size', 0) / (1024**3),
                'mcool_href': mf.get('href', ''),
                'mcool_download_url': mf.get('download_url', ''),
                'mcool_source': mf.get('source', '4DN'),
                'experiment_set': mf.get('experiment_set', ''),
                'ccre_accession': ccre.get('accession', ''),
                'ccre_size': ccre.get('file_size', 0),
                'ccre_size_mb': ccre.get('file_size', 0) / (1024**2),
                'ccre_href': ccre.get('href', ''),
                'ccre_download_url': ccre.get('download_url', ''),
                'ccre_assembly': ccre.get('assembly', 'GRCh38'),
                'total_size': mf.get('file_size', 0) + ccre.get('file_size', 0),
                'total_size_gb': (mf.get('file_size', 0) + ccre.get('file_size', 0)) / (1024**3),
            })

        # Apply filters
        if tissues:
            tissue_lower = [t.lower() for t in tissues]
            paired = [p for p in paired if p['tissue'].lower() in tissue_lower]

        if min_size_gb is not None:
            paired = [p for p in paired if p['mcool_size_gb'] >= min_size_gb]
        if max_size_gb is not None:
            paired = [p for p in paired if p['mcool_size_gb'] <= max_size_gb]

        if one_per_cell:
            best = {}
            for p in paired:
                cell = p['cell_line']
                if cell not in best or p['mcool_size'] > best[cell]['mcool_size']:
                    best[cell] = p
            paired = list(best.values())

        # Replicate filters (applied after one_per_cell grouping)
        if min_replicates is not None or max_replicates is not None:
            # Count replicates per cell line
            from collections import Counter
            cell_counts = Counter(p['cell_line'] for p in paired)
            if min_replicates is not None:
                paired = [p for p in paired if cell_counts[p['cell_line']] >= min_replicates]
            if max_replicates is not None:
                paired = [p for p in paired if cell_counts[p['cell_line']] <= max_replicates]

        return paired

    # ------------------------------------------------------------------
    # Non-paired (mcool only, no cCRE match)
    # ------------------------------------------------------------------
    def find_non_paired_datasets(self, cell_line_filter=None):
        """Find mcool files that do NOT have a matching cCRE."""
        mcool_files = self.fetch_4dn_mcool_files(cell_line_filter=cell_line_filter)
        mcool_files = [f for f in mcool_files if f.get('cell_line', '').lower().strip() != 'unknown']

        for mf in mcool_files:
            mf['cell_line_raw'] = mf.get('cell_line', '')
            mf['cell_line_normalized'] = mf.get('cell_line_normalized') or normalize_cell_line(mf['cell_line'])

        unique_norm_cells = sorted(set(f['cell_line_normalized'] for f in mcool_files))
        ccre_map = self.fetch_all_ccre(unique_norm_cells)

        non_paired = []
        for mf in mcool_files:
            norm_cell = mf['cell_line_normalized']
            if ccre_map.get(norm_cell) is None:
                non_paired.append({
                    'cell_line': norm_cell,
                    'cell_line_raw': mf['cell_line_raw'],
                    'tissue': get_tissue(norm_cell),
                    'accession': mf.get('accession', ''),
                    'file_size': mf.get('file_size', 0),
                    'file_size_gb': mf.get('file_size', 0) / (1024**3),
                    'download_url': mf.get('download_url', ''),
                    'source': mf.get('source', '4DN'),
                })
        return non_paired

    # ------------------------------------------------------------------
    # Download with parallel support
    # ------------------------------------------------------------------
    def download_file(self, url, out_path, accession='', resume=True, progress_callback=None):
        """Download a file with resume support. Returns output path on success, None on failure."""
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        if out_path.exists() and not resume:
            return str(out_path)

        headers = {}
        existing_size = 0
        if resume and out_path.exists():
            existing_size = out_path.stat().st_size
            headers['Range'] = f'bytes={existing_size}-'

        # Pick session based on URL
        session = self.session_4dn if FOURDN_API in url else self.session_encode

        try:
            r = session.get(url, stream=True, headers=headers, timeout=300, allow_redirects=True)

            if r.status_code == 200 and existing_size > 0:
                existing_size = 0
            elif r.status_code == 416:
                return str(out_path)

            r.raise_for_status()

            total_expected = int(r.headers.get('content-length', 0)) + existing_size
            total_written = existing_size
            start_time = time.time()

            mode = 'ab' if existing_size > 0 and r.status_code == 206 else 'wb'
            with open(out_path, mode) as f:
                for chunk in r.iter_content(chunk_size=4 * 1024 * 1024):
                    f.write(chunk)
                    total_written += len(chunk)

                    if progress_callback:
                        elapsed = time.time() - start_time
                        speed = (total_written - existing_size) / elapsed / (1024 * 1024) if elapsed > 0 else 0
                        pct = (total_written / total_expected * 100) if total_expected > 0 else 0
                        progress_callback({
                            'accession': accession,
                            'bytes_written': total_written,
                            'bytes_total': total_expected,
                            'percent': round(pct, 1),
                            'speed_mbps': round(speed, 1),
                        })

            return str(out_path)

        except Exception as e:
            self._log(f"Download failed for {accession}: {e}")
            return None

    def download_paired_parallel(self, paired_list, resume=True, max_workers=None, progress_callback=None):
        """Download multiple paired datasets in parallel."""
        if max_workers is None:
            max_workers = MAX_PARALLEL

        download_tasks = []
        for p in paired_list:
            cell_safe = p['cell_line'].replace(' ', '_').replace(',', '').replace('/', '_')

            # mcool task
            mcool_path = MCOOL_DIR / f"{cell_safe}_{p['mcool_accession']}.mcool"
            download_tasks.append({
                'type': 'mcool',
                'cell_line': p['cell_line'],
                'accession': p['mcool_accession'],
                'url': p['mcool_download_url'],
                'path': str(mcool_path),
                'size': p['mcool_size'],
            })

            # ccre task
            ccre_path = CCRE_DIR / f"{cell_safe}_{p['ccre_accession']}.bed.gz"
            download_tasks.append({
                'type': 'ccre',
                'cell_line': p['cell_line'],
                'accession': p['ccre_accession'],
                'url': p['ccre_download_url'],
                'path': str(ccre_path),
                'size': p['ccre_size'],
            })

        results = []

        def _do_download(task):
            result_path = self.download_file(
                task['url'], task['path'],
                accession=task['accession'],
                resume=resume,
                progress_callback=progress_callback,
            )
            success = result_path is not None
            if success:
                from . import db
                file_type = 'chromatin' if task['type'] == 'mcool' else 'regulatory'
                db.mark_downloaded(self.con, task['accession'], result_path, file_type)
            return {**task, 'success': success, 'local_path': result_path}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_do_download, t): t for t in download_tasks}
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    task = futures[future]
                    results.append({**task, 'success': False, 'error': str(e)})

        return results

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------
    def validate_bed_gz(self, filepath):
        """Quick validation of a .bed.gz file."""
        try:
            with gzip.open(filepath, 'rt') as f:
                first_line = f.readline().strip()
                if first_line:
                    cols = first_line.split('\t')
                    if len(cols) >= 3:
                        return {'valid': True, 'columns': len(cols), 'first_line': first_line}
            return {'valid': False, 'error': 'Empty or invalid'}
        except Exception as e:
            return {'valid': False, 'error': str(e)}

    def validate_mcool(self, filepath):
        """Quick validation of a .mcool file (check HDF5 magic bytes)."""
        try:
            with open(filepath, 'rb') as f:
                magic = f.read(8)
                if magic[:4] == b'\x89HDF':
                    return {'valid': True, 'format': 'HDF5/mcool'}
                return {'valid': False, 'error': 'Not a valid HDF5 file'}
        except Exception as e:
            return {'valid': False, 'error': str(e)}

    # ------------------------------------------------------------------
    # Comprehensive dataset validation for ML training
    # ------------------------------------------------------------------
    def validate_datasets(self, species_filter=None):
        """Validate all downloaded datasets for ML training readiness.
        Checks: genome assembly consistency, file integrity, cCRE pairing, treatment status.
        """
        results = {
            'validated': [],
            'issues': [],
            'summary': {
                'total_checked': 0,
                'valid': 0,
                'invalid': 0,
                'assembly_mismatches': 0,
                'missing_ccre': 0,
                'missing_mcool': 0,
            }
        }

        # Get all cell lines with their experiments
        query = """
            SELECT cl.cell_line_normalized as cell_line, cl.species, cl.genome_assembly,
                   cl.has_ccre, cl.has_mcool,
                   ce.accession as mcool_accession, ce.download_status as mcool_dl_status,
                   ce.local_path as mcool_path, ce.genome_assembly as exp_assembly,
                   ce.treatment, ce.modification, ce.condition,
                   ra.accession as ccre_accession, ra.download_status as ccre_dl_status,
                   ra.local_path as ccre_path, ra.assembly as ccre_assembly
            FROM cell_lines cl
            LEFT JOIN chromatin_experiments ce ON cl.cell_line_id = ce.cell_line_id
            LEFT JOIN regulatory_annotations ra ON cl.cell_line_id = ra.cell_line_id
            WHERE cl.has_mcool = TRUE
        """
        params = []
        if species_filter:
            query += " AND cl.species = ?"
            params.append(species_filter)
        query += " ORDER BY cl.cell_line_normalized"

        try:
            rows = self.con.execute(query, params).fetchdf()
        except Exception as e:
            results['issues'].append({'type': 'query_error', 'message': str(e)})
            return results

        for _, row in rows.iterrows():
            results['summary']['total_checked'] += 1
            entry = {
                'cell_line': row.get('cell_line', ''),
                'species': row.get('species', ''),
                'genome_assembly': row.get('genome_assembly', ''),
                'mcool_accession': row.get('mcool_accession', ''),
                'ccre_accession': row.get('ccre_accession', ''),
                'checks': [],
                'valid': True,
            }

            # Check 1: Assembly consistency
            exp_asm = row.get('exp_assembly', '') or ''
            ccre_asm = row.get('ccre_assembly', '') or ''
            cl_asm = row.get('genome_assembly', '') or ''

            if ccre_asm and exp_asm and ccre_asm != cl_asm:
                entry['checks'].append({
                    'check': 'assembly_match',
                    'status': 'warning',
                    'message': f'cCRE assembly ({ccre_asm}) differs from expected ({cl_asm})',
                })
                results['summary']['assembly_mismatches'] += 1
            else:
                entry['checks'].append({'check': 'assembly_match', 'status': 'ok', 'message': f'Assembly: {cl_asm}'})

            # Check 2: cCRE availability
            has_ccre = bool(row.get('has_ccre'))
            if not has_ccre:
                entry['checks'].append({
                    'check': 'ccre_available',
                    'status': 'missing',
                    'message': 'No cCRE annotation found for this cell line',
                })
                entry['valid'] = False
                results['summary']['missing_ccre'] += 1
            else:
                entry['checks'].append({'check': 'ccre_available', 'status': 'ok', 'message': f'cCRE: {row.get("ccre_accession", "")}'})

            # Check 3: mcool file integrity (if downloaded)
            mcool_path = row.get('mcool_path', '') or ''
            if mcool_path and Path(mcool_path).exists():
                validation = self.validate_mcool(mcool_path)
                if validation.get('valid'):
                    entry['checks'].append({'check': 'mcool_integrity', 'status': 'ok', 'message': 'HDF5 valid'})
                else:
                    entry['checks'].append({
                        'check': 'mcool_integrity',
                        'status': 'error',
                        'message': f'Invalid mcool: {validation.get("error", "unknown")}',
                    })
                    entry['valid'] = False
            elif row.get('mcool_dl_status') == 'downloaded':
                entry['checks'].append({
                    'check': 'mcool_integrity',
                    'status': 'warning',
                    'message': 'Marked downloaded but file not found',
                })
            else:
                entry['checks'].append({'check': 'mcool_integrity', 'status': 'pending', 'message': 'Not yet downloaded'})

            # Check 4: cCRE file integrity (if downloaded)
            ccre_path = row.get('ccre_path', '') or ''
            if ccre_path and Path(ccre_path).exists():
                validation = self.validate_bed_gz(ccre_path)
                if validation.get('valid'):
                    entry['checks'].append({'check': 'ccre_integrity', 'status': 'ok',
                                            'message': f'BED valid ({validation.get("columns", 0)} cols)'})
                else:
                    entry['checks'].append({
                        'check': 'ccre_integrity',
                        'status': 'error',
                        'message': f'Invalid BED: {validation.get("error", "unknown")}',
                    })
                    entry['valid'] = False
            elif has_ccre:
                entry['checks'].append({'check': 'ccre_integrity', 'status': 'pending', 'message': 'Not yet downloaded'})

            # Check 5: Treatment status (informational)
            treatment = row.get('treatment', '') or ''
            if treatment:
                entry['checks'].append({
                    'check': 'treatment',
                    'status': 'info',
                    'message': f'Treatment: {treatment[:60]}',
                })
                entry['treatment_warning'] = 'cCRE may not match treated chromatin state'

            if entry['valid']:
                results['summary']['valid'] += 1
            else:
                results['summary']['invalid'] += 1

            results['validated'].append(entry)

        return results

    # ------------------------------------------------------------------
    # Statistics (for the Ink UI)
    # ------------------------------------------------------------------
    def get_stats(self):
        """Get comprehensive statistics from DuckDB."""
        stats = {}

        # Summary
        row = self.con.execute("SELECT * FROM stats_summary").fetchone()
        if row:
            cols = [d[0] for d in self.con.description]
            stats['summary'] = dict(zip(cols, row))
        else:
            stats['summary'] = {}

        # Per cell line (with species + assembly)
        try:
            cell_stats = self.con.execute("""
                SELECT cl.cell_line_normalized as cell_line, cl.tissue_type as tissue,
                       cl.species, cl.genome_assembly,
                       COUNT(DISTINCT ce.experiment_id) as replicates,
                       COALESCE(SUM(ce.file_size_gb), 0) as total_gb,
                       cl.has_ccre, cl.has_mcool, cl.biosample_type
                FROM cell_lines cl
                LEFT JOIN chromatin_experiments ce ON cl.cell_line_id = ce.cell_line_id
                GROUP BY cl.cell_line_normalized, cl.tissue_type, cl.species,
                         cl.genome_assembly, cl.has_ccre, cl.has_mcool, cl.biosample_type
                ORDER BY replicates DESC
            """).fetchdf()
            stats['cell_lines'] = cell_stats.to_dict(orient='records')
        except Exception:
            stats['cell_lines'] = []

        # Species breakdown
        try:
            species_stats = self.con.execute("""
                SELECT cl.species, COUNT(*) as cell_line_count,
                       SUM(cl.total_experiments) as experiment_count,
                       SUM(CASE WHEN cl.has_ccre THEN 1 ELSE 0 END) as with_ccre
                FROM cell_lines cl
                GROUP BY cl.species
                ORDER BY experiment_count DESC
            """).fetchdf()
            stats['species'] = species_stats.to_dict(orient='records')
        except Exception:
            stats['species'] = []

        # Treatment summary
        try:
            treatment_stats = self.con.execute("""
                SELECT
                    CASE WHEN treatment = '' OR treatment IS NULL THEN 'untreated'
                         ELSE treatment END as treatment,
                    COUNT(*) as count,
                    COUNT(DISTINCT cell_line_name) as cell_lines
                FROM chromatin_experiments
                GROUP BY treatment
                ORDER BY count DESC
                LIMIT 20
            """).fetchdf()
            stats['treatments'] = treatment_stats.to_dict(orient='records')
        except Exception:
            stats['treatments'] = []

        # Tissue distribution
        try:
            tissue_stats = self.con.execute("""
                SELECT tissue_type as tissue, COUNT(*) as count
                FROM cell_lines
                WHERE tissue_type != 'Unknown'
                GROUP BY tissue_type
                ORDER BY count DESC
            """).fetchdf()
            stats['tissues'] = tissue_stats.to_dict(orient='records')
        except Exception:
            stats['tissues'] = []

        # Size distribution
        try:
            size_dist = self.con.execute("""
                SELECT
                    CASE
                        WHEN file_size_gb < 1 THEN '< 1 GB'
                        WHEN file_size_gb < 10 THEN '1-10 GB'
                        WHEN file_size_gb < 30 THEN '10-30 GB'
                        WHEN file_size_gb < 50 THEN '30-50 GB'
                        ELSE '> 50 GB'
                    END as size_range,
                    COUNT(*) as count
                FROM chromatin_experiments
                GROUP BY size_range
                ORDER BY MIN(file_size_gb)
            """).fetchdf()
            stats['size_distribution'] = size_dist.to_dict(orient='records')
        except Exception:
            stats['size_distribution'] = []

        # Download status
        try:
            dl_stats = self.con.execute("""
                SELECT download_status as status, COUNT(*) as count
                FROM chromatin_experiments
                GROUP BY download_status
            """).fetchdf()
            stats['download_status'] = dl_stats.to_dict(orient='records')
        except Exception:
            stats['download_status'] = []

        # Paired datasets
        try:
            paired = self.con.execute("SELECT * FROM paired_datasets ORDER BY total_size_gb DESC").fetchdf()
            stats['paired'] = paired.to_dict(orient='records')
        except Exception:
            stats['paired'] = []

        return stats

    # ------------------------------------------------------------------
    # Duplicate check
    # ------------------------------------------------------------------
    def check_duplicates(self):
        """Identify duplicate vs unique experiments."""
        try:
            df = self.con.execute("""
                SELECT cell_line_name as cell_line,
                       COUNT(*) as replicate_count,
                       SUM(file_size_gb) as total_size_gb
                FROM chromatin_experiments
                GROUP BY cell_line_name
                ORDER BY replicate_count DESC
            """).fetchdf()
            df['is_unique'] = df['replicate_count'] == 1
            df['is_duplicate'] = df['replicate_count'] > 1
            return df
        except Exception:
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # Report generation
    # ------------------------------------------------------------------
    def generate_report(self, one_per_cell=False, output_csv=None):
        """Generate a full paired dataset report."""
        paired = self.find_paired_datasets(one_per_cell=one_per_cell)
        if not paired:
            return None

        df = pd.DataFrame(paired)
        csv_path = output_csv or str(REPORT_DIR / "paired_dataset_report.csv")
        df.to_csv(csv_path, index=False)

        # Summary
        summary = df.groupby('cell_line').agg({
            'mcool_accession': 'count',
            'mcool_size_gb': 'sum',
            'ccre_size_mb': 'first',
            'total_size_gb': 'sum'
        }).reset_index()
        summary.columns = ['cell_line', 'samples', 'mcool_gb', 'ccre_mb', 'total_gb']
        summary = summary.sort_values('total_gb', ascending=False)

        summary_path = str(REPORT_DIR / "paired_summary_by_cell.csv")
        summary.to_csv(summary_path, index=False)

        # Export Parquet
        from . import db
        try:
            db.export_parquet(self.con, 'chromatin_experiments', str(RAW_DIR / '4dn_experiments.parquet'))
            db.export_parquet(self.con, 'regulatory_annotations', str(RAW_DIR / 'encode_annotations.parquet'))
        except Exception:
            pass

        return {
            'report_path': csv_path,
            'summary_path': summary_path,
            'total_cell_lines': df['cell_line'].nunique(),
            'total_samples': len(df),
            'total_mcool_gb': round(df['mcool_size_gb'].sum(), 2),
            'total_ccre_mb': round(df['ccre_size_mb'].sum(), 1),
            'total_gb': round(df['total_size_gb'].sum(), 2),
        }
