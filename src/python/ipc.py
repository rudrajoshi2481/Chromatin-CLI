#!/usr/bin/env python3
"""
JSON IPC Bridge for Ink CLI
============================
Accepts JSON commands on stdin, returns JSON responses on stdout.
This is the interface between the TypeScript Ink frontend and the Python backend.

Protocol:
  Input:  {"action": "...", "params": {...}}
  Output: {"ok": true, "data": {...}} or {"ok": false, "error": "..."}
"""

import sys
import json
import os
import traceback
from pathlib import Path
from dotenv import load_dotenv

# Ensure we can import sibling modules
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
load_dotenv(Path(__file__).resolve().parent.parent.parent / '.env')

from src.python import db
from src.python.data_manager import DataManager, _sizeof_fmt


def handle_action(action, params, manager):
    """Route an action to the appropriate handler."""

    if action == 'ping':
        return {'ok': True, 'data': {'status': 'alive', 'version': '2.0.0'}}

    elif action == 'get_stats':
        stats = manager.get_stats()
        return {'ok': True, 'data': stats}

    elif action == 'fetch_mcool':
        cell_line = params.get('cell_line')
        force = params.get('force_refresh', False)
        files = manager.fetch_4dn_mcool_files(cell_line_filter=cell_line, force_refresh=force)
        # Make JSON-safe
        return {'ok': True, 'data': {'files': files, 'count': len(files)}}

    elif action == 'fetch_all_data':
        # Step 1: Fetch mcool from 4DN
        force = params.get('force_refresh', False)
        progress_msg = lambda msg: print(json.dumps({'type': 'progress', 'message': msg}), file=sys.stderr, flush=True)

        progress_msg('Fetching .mcool files from 4DN...')
        files = manager.fetch_4dn_mcool_files(force_refresh=force)
        mcool_count = len(files)
        progress_msg(f'Found {mcool_count} .mcool files from 4DN')

        # Step 2: Get unique normalized cell lines
        from src.python.data_manager import normalize_cell_line
        unique_cells = sorted(set(
            normalize_cell_line(f.get('cell_line', 'Unknown'))
            for f in files
            if f.get('cell_line', '').lower().strip() != 'unknown'
        ))

        # Step 3: Fetch cCRE from ENCODE for each unique cell line
        progress_msg(f'Fetching cCRE from ENCODE for {len(unique_cells)} cell lines...')
        ccre_found = 0
        ccre_total = len(unique_cells)
        for i, cell in enumerate(unique_cells, 1):
            progress_msg(f'[{i}/{ccre_total}] Querying ENCODE cCRE for: {cell}')
            try:
                result = manager.fetch_ccre_for_cell_line(cell)
                if result and result.get('accession'):
                    ccre_found += 1
            except Exception as e:
                progress_msg(f'Error fetching cCRE for {cell}: {e}')
            import time
            time.sleep(0.3)

        progress_msg(f'Found cCRE data for {ccre_found}/{ccre_total} cell lines')

        # Step 4: Get updated stats
        stats = manager.get_stats()
        return {'ok': True, 'data': {
            'mcool_count': mcool_count,
            'ccre_found': ccre_found,
            'ccre_total': ccre_total,
            'stats': stats,
        }}

    elif action == 'fetch_ccre':
        cell_line = params.get('cell_line', '')
        if not cell_line:
            return {'ok': False, 'error': 'cell_line is required'}
        result = manager.fetch_ccre_for_cell_line(cell_line)
        return {'ok': True, 'data': result}

    elif action == 'find_paired':
        paired = manager.find_paired_datasets(
            cell_line_filter=params.get('cell_line'),
            one_per_cell=params.get('one_per_cell', False),
            min_replicates=params.get('min_replicates'),
            max_replicates=params.get('max_replicates'),
            min_size_gb=params.get('min_size_gb'),
            max_size_gb=params.get('max_size_gb'),
            tissues=params.get('tissues'),
        )
        return {'ok': True, 'data': {'paired': paired, 'count': len(paired)}}

    elif action == 'find_non_paired':
        non_paired = manager.find_non_paired_datasets(
            cell_line_filter=params.get('cell_line'),
        )
        return {'ok': True, 'data': {'non_paired': non_paired, 'count': len(non_paired)}}

    elif action == 'check_duplicates':
        df = manager.check_duplicates()
        records = df.to_dict(orient='records') if not df.empty else []
        return {'ok': True, 'data': {'duplicates': records}}

    elif action == 'list_cell_lines':
        species_filter = params.get('species')  # e.g. 'Homo sapiens', 'Mus musculus'
        ccre_only = params.get('ccre_only', False)
        try:
            query = """
                SELECT cl.cell_line_normalized as cell_line, cl.tissue_type as tissue,
                       cl.species, cl.genome_assembly, cl.biosample_type,
                       cl.has_ccre, cl.has_mcool, cl.total_experiments as replicates,
                       COALESCE(SUM(ce.file_size_gb), 0) as total_gb,
                       COUNT(DISTINCT CASE WHEN ce.treatment != '' AND ce.treatment IS NOT NULL
                             THEN ce.experiment_id END) as treated_count,
                       COUNT(DISTINCT CASE WHEN ce.treatment = '' OR ce.treatment IS NULL
                             THEN ce.experiment_id END) as untreated_count
                FROM cell_lines cl
                LEFT JOIN chromatin_experiments ce ON cl.cell_line_id = ce.cell_line_id
                WHERE 1=1
            """
            qparams = []
            if species_filter:
                query += " AND cl.species = ?"
                qparams.append(species_filter)
            if ccre_only:
                query += " AND cl.has_ccre = TRUE"
            query += """
                GROUP BY cl.cell_line_normalized, cl.tissue_type, cl.species,
                         cl.genome_assembly, cl.biosample_type, cl.has_ccre, cl.has_mcool,
                         cl.total_experiments
                ORDER BY cl.total_experiments DESC
            """
            rows = manager.con.execute(query, qparams).fetchdf()
            return {'ok': True, 'data': {'cell_lines': rows.to_dict(orient='records')}}
        except Exception as e:
            return {'ok': True, 'data': {'cell_lines': []}}

    elif action == 'get_cell_line_details':
        """Get detailed info for a cell line: all replicates, treatments, cCRE data."""
        cell_line = params.get('cell_line', '')
        if not cell_line:
            return {'ok': False, 'error': 'cell_line is required'}
        try:
            # Cell line summary
            cl_row = manager.con.execute("""
                SELECT cl.cell_line_id, cl.cell_line_normalized as cell_line,
                       cl.tissue_type as tissue, cl.species, cl.genome_assembly,
                       cl.biosample_type, cl.has_ccre, cl.has_mcool, cl.total_experiments
                FROM cell_lines cl
                WHERE cl.cell_line_normalized = ?
            """, [cell_line]).fetchdf()
            if cl_row.empty:
                return {'ok': False, 'error': f'Cell line not found: {cell_line}'}
            cl_info = cl_row.to_dict(orient='records')[0]
            cl_id = cl_info['cell_line_id']

            # All replicates (chromatin experiments)
            reps = manager.con.execute("""
                SELECT ce.experiment_id, ce.accession, ce.file_size_gb,
                       ce.download_status, ce.local_path, ce.treatment,
                       ce.treatment_duration, ce.modification, ce.condition,
                       ce.biosample_type, ce.study, ce.dataset_label,
                       ce.genome_assembly, ce.experiment_set, ce.date_added
                FROM chromatin_experiments ce
                WHERE ce.cell_line_id = ?
                ORDER BY ce.file_size_gb DESC
            """, [cl_id]).fetchdf()
            replicates = reps.to_dict(orient='records')

            # cCRE annotations
            ccre = manager.con.execute("""
                SELECT ra.annotation_id, ra.accession, ra.file_size_mb,
                       ra.download_status, ra.local_path, ra.assembly,
                       ra.output_type, ra.date_added
                FROM regulatory_annotations ra
                WHERE ra.cell_line_id = ?
                ORDER BY ra.file_size_mb DESC
            """, [cl_id]).fetchdf()
            ccre_list = ccre.to_dict(orient='records')

            # Treatment summary
            treatments = {}
            for r in replicates:
                tx = r.get('treatment', '') or 'untreated'
                if tx not in treatments:
                    treatments[tx] = {'count': 0, 'total_gb': 0}
                treatments[tx]['count'] += 1
                treatments[tx]['total_gb'] += r.get('file_size_gb', 0) or 0
            treatment_summary = [
                {'treatment': k, 'count': v['count'], 'total_gb': round(v['total_gb'], 2)}
                for k, v in sorted(treatments.items(), key=lambda x: -x[1]['count'])
            ]

            total_gb = sum(r.get('file_size_gb', 0) or 0 for r in replicates)
            downloaded = sum(1 for r in replicates if r.get('download_status') == 'downloaded')

            return {'ok': True, 'data': {
                'cell_line': cl_info,
                'replicates': replicates,
                'ccre': ccre_list,
                'treatment_summary': treatment_summary,
                'summary': {
                    'total_replicates': len(replicates),
                    'total_gb': round(total_gb, 2),
                    'downloaded': downloaded,
                    'ccre_count': len(ccre_list),
                    'unique_treatments': len(treatments),
                },
            }}
        except Exception as e:
            return {'ok': False, 'error': str(e)}

    elif action == 'list_ml_ready':
        """Find datasets ready for ML training: paired (mcool + cCRE), with species/assembly info."""
        species_filter = params.get('species', 'Homo sapiens')
        one_per_cell = params.get('one_per_cell', False)
        try:
            query = """
                SELECT
                    cl.cell_line_normalized as cell_line,
                    cl.tissue_type as tissue,
                    cl.species,
                    cl.genome_assembly,
                    cl.biosample_type,
                    ce.accession as mcool_accession,
                    ce.file_size_gb as mcool_size_gb,
                    ce.download_url as mcool_url,
                    ce.treatment,
                    ce.modification,
                    ce.condition,
                    ce.download_status as mcool_status,
                    ra.accession as ccre_accession,
                    ra.file_size_mb as ccre_size_mb,
                    ra.download_url as ccre_url,
                    ra.assembly as ccre_assembly,
                    ra.download_status as ccre_status,
                    CASE
                        WHEN ce.download_status = 'downloaded' AND ra.download_status = 'downloaded'
                        THEN 'ready'
                        WHEN ce.download_status = 'downloaded' OR ra.download_status = 'downloaded'
                        THEN 'partial'
                        ELSE 'pending'
                    END as ml_status
                FROM cell_lines cl
                JOIN chromatin_experiments ce ON cl.cell_line_id = ce.cell_line_id
                JOIN regulatory_annotations ra ON cl.cell_line_id = ra.cell_line_id
                WHERE cl.has_ccre = TRUE AND cl.has_mcool = TRUE
            """
            qparams = []
            if species_filter:
                query += " AND cl.species = ?"
                qparams.append(species_filter)
            query += " ORDER BY ce.file_size_gb DESC"

            rows = manager.con.execute(query, qparams).fetchdf()
            records = rows.to_dict(orient='records')

            if one_per_cell:
                best = {}
                for r in records:
                    cell = r['cell_line']
                    if cell not in best or r['mcool_size_gb'] > best[cell]['mcool_size_gb']:
                        best[cell] = r
                records = list(best.values())

            # Summary
            unique_cells = set(r['cell_line'] for r in records)
            total_gb = sum(r.get('mcool_size_gb', 0) for r in records)
            treated = [r for r in records if r.get('treatment')]
            untreated = [r for r in records if not r.get('treatment')]

            return {'ok': True, 'data': {
                'datasets': records,
                'count': len(records),
                'unique_cell_lines': len(unique_cells),
                'total_gb': round(total_gb, 2),
                'treated_count': len(treated),
                'untreated_count': len(untreated),
                'species': species_filter or 'all',
            }}
        except Exception as e:
            return {'ok': False, 'error': str(e)}

    elif action == 'download_paired':
        cell_line = params.get('cell_line')
        limit = params.get('limit')
        one_per_cell = params.get('one_per_cell', True)
        paired = manager.find_paired_datasets(
            cell_line_filter=cell_line,
            one_per_cell=one_per_cell,
            min_size_gb=params.get('min_size_gb'),
            max_size_gb=params.get('max_size_gb'),
            tissues=params.get('tissues'),
        )
        if limit:
            paired = paired[:int(limit)]

        if not paired:
            return {'ok': False, 'error': f'No paired datasets found for filter: {cell_line or "all"}'}

        # Stream progress via stderr
        def progress_cb(info):
            msg = json.dumps({'type': 'progress', **info})
            print(msg, file=sys.stderr, flush=True)

        results = manager.download_paired_parallel(
            paired, resume=params.get('resume', True),
            max_workers=params.get('max_workers'),
            progress_callback=progress_cb,
        )
        return {'ok': True, 'data': {'results': results, 'total': len(results),
                                      'success': sum(1 for r in results if r.get('success'))}}

    elif action == 'download_mcool_only':
        files = manager.fetch_4dn_mcool_files(cell_line_filter=params.get('cell_line'))
        files = [f for f in files if f.get('cell_line', '').lower().strip() != 'unknown']
        limit = params.get('limit')
        if limit:
            files = files[:int(limit)]

        download_tasks = []
        for f in files:
            cell_safe = f.get('cell_line_normalized', f.get('cell_line', 'unknown')).replace(' ', '_')
            path = str(Path(os.getenv("DATA_DIR", "./data")) / "downloads" / "mcool" / f"{cell_safe}_{f['accession']}.mcool")
            download_tasks.append({
                'type': 'mcool',
                'cell_line': f.get('cell_line', ''),
                'accession': f.get('accession', ''),
                'url': f.get('download_url', ''),
                'path': path,
                'size': f.get('file_size', 0),
            })

        results = []
        for task in download_tasks:
            result_path = manager.download_file(task['url'], task['path'], accession=task['accession'])
            results.append({**task, 'success': result_path is not None})

        return {'ok': True, 'data': {'results': results, 'total': len(results)}}

    elif action == 'generate_report':
        report = manager.generate_report(
            one_per_cell=params.get('one_per_cell', False),
            output_csv=params.get('output_csv'),
        )
        return {'ok': True, 'data': report}

    elif action == 'query':
        sql = params.get('sql', '')
        if not sql:
            return {'ok': False, 'error': 'sql is required'}
        # Safety: only allow SELECT
        if not sql.strip().upper().startswith('SELECT'):
            return {'ok': False, 'error': 'Only SELECT queries are allowed'}
        try:
            df = manager.con.execute(sql).fetchdf()
            return {'ok': True, 'data': {'rows': df.to_dict(orient='records'), 'count': len(df)}}
        except Exception as e:
            return {'ok': False, 'error': str(e)}

    elif action == 'get_download_status':
        """Get download status for all files."""
        try:
            chromatin_dl = manager.con.execute("""
                SELECT ce.accession, ce.cell_line_name, ce.file_size_gb,
                       ce.download_status, ce.local_path, ce.date_downloaded
                FROM chromatin_experiments ce
                WHERE ce.download_status = 'downloaded'
                ORDER BY ce.date_downloaded DESC
            """).fetchdf()
            ccre_dl = manager.con.execute("""
                SELECT ra.accession, ra.cell_line_name, ra.file_size_mb,
                       ra.download_status, ra.local_path
                FROM regulatory_annotations ra
                WHERE ra.download_status = 'downloaded'
            """).fetchdf()
            pending_chr = manager.con.execute(
                "SELECT COUNT(*) FROM chromatin_experiments WHERE download_status = 'pending'"
            ).fetchone()[0]
            pending_ccre = manager.con.execute(
                "SELECT COUNT(*) FROM regulatory_annotations WHERE download_status = 'pending'"
            ).fetchone()[0]

            return {'ok': True, 'data': {
                'downloaded_chromatin': chromatin_dl.to_dict(orient='records'),
                'downloaded_ccre': ccre_dl.to_dict(orient='records'),
                'counts': {
                    'downloaded_chromatin': len(chromatin_dl),
                    'downloaded_ccre': len(ccre_dl),
                    'pending_chromatin': pending_chr,
                    'pending_ccre': pending_ccre,
                }
            }}
        except Exception as e:
            return {'ok': False, 'error': str(e)}

    elif action == 'start_download_job':
        """Start a background download job (nohup process).
        Pre-resolves all URLs/paths from DuckDB so the worker is DB-free.
        """
        import subprocess, uuid
        items = params.get('items', [])  # [{cell_line, accessions?, include_ccre?}, ...]
        if not items:
            return {'ok': False, 'error': 'items is required (list of {cell_line, accessions?, include_ccre?})'}

        job_id = params.get('job_id') or str(uuid.uuid4())[:8]
        jobs_dir = Path(os.getenv("DATA_DIR", "./data")) / "jobs"
        jobs_dir.mkdir(parents=True, exist_ok=True)
        data_dir = Path(os.getenv("DATA_DIR", "./data"))

        # Pre-resolve all download tasks from DuckDB
        tasks = []
        for item in items:
            cell_line = item.get('cell_line', '')
            accessions = item.get('accessions', [])
            include_ccre = item.get('include_ccre', True)
            cell_safe = cell_line.replace(' ', '_').replace('/', '_')

            if accessions:
                for acc in accessions:
                    row = manager.con.execute("""
                        SELECT ce.accession, ce.download_url, ce.file_size_bytes, ce.file_size_gb
                        FROM chromatin_experiments ce WHERE ce.accession = ?
                    """, [acc]).fetchone()
                    if row:
                        tasks.append({
                            'type': 'mcool', 'cell_line': cell_line,
                            'accession': row[0], 'url': row[1],
                            'path': str(data_dir / "downloads" / "mcool" / f"{cell_safe}_{row[0]}.mcool"),
                            'size': row[2] or 0, 'size_gb': row[3] or 0,
                        })
            else:
                rows = manager.con.execute("""
                    SELECT ce.accession, ce.download_url, ce.file_size_bytes, ce.file_size_gb
                    FROM chromatin_experiments ce
                    JOIN cell_lines cl ON ce.cell_line_id = cl.cell_line_id
                    WHERE cl.cell_line_normalized = ?
                    ORDER BY ce.file_size_gb DESC
                """, [cell_line]).fetchall()
                for row in rows:
                    tasks.append({
                        'type': 'mcool', 'cell_line': cell_line,
                        'accession': row[0], 'url': row[1],
                        'path': str(data_dir / "downloads" / "mcool" / f"{cell_safe}_{row[0]}.mcool"),
                        'size': row[2] or 0, 'size_gb': row[3] or 0,
                    })

            if include_ccre:
                ccre_rows = manager.con.execute("""
                    SELECT ra.accession, ra.download_url, ra.file_size_mb
                    FROM regulatory_annotations ra
                    JOIN cell_lines cl ON ra.cell_line_id = cl.cell_line_id
                    WHERE cl.cell_line_normalized = ?
                """, [cell_line]).fetchall()
                for row in ccre_rows:
                    tasks.append({
                        'type': 'ccre', 'cell_line': cell_line,
                        'accession': row[0], 'url': row[1],
                        'path': str(data_dir / "downloads" / "ccre" / f"{cell_safe}_{row[0]}.bed.gz"),
                        'size': int((row[2] or 0) * 1024 * 1024),
                        'size_gb': (row[2] or 0) / 1024,
                    })

        if not tasks:
            return {'ok': False, 'error': 'No downloadable files found for the given items'}

        # Write tasks to a file (worker reads this, not DuckDB)
        tasks_file = str(jobs_dir / f"{job_id}_tasks.json")
        with open(tasks_file, 'w') as f:
            json.dump(tasks, f, default=str)

        job_file = str(jobs_dir / f"{job_id}.json")
        with open(job_file, 'w') as f:
            json.dump({
                'job_id': job_id,
                'status': 'starting',
                'created_at': __import__('datetime').datetime.now().isoformat(),
                'total_tasks': len(tasks),
                'total_gb': round(sum(t.get('size_gb', 0) for t in tasks), 2),
                'cell_lines': list(set(t['cell_line'] for t in tasks)),
            }, f, default=str)

        # Launch detached background process
        worker_script = str(Path(__file__).resolve().parent / 'download_worker.py')
        log_file = str(jobs_dir / f"{job_id}.log")

        proc = subprocess.Popen(
            ['python3', worker_script, job_id, job_file, tasks_file],
            cwd=str(Path(__file__).resolve().parent.parent.parent),
            stdout=open(log_file, 'w'),
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

        # Update job file with PID
        with open(job_file, 'r') as f:
            job = json.load(f)
        job['pid'] = proc.pid
        job['log_file'] = log_file
        with open(job_file, 'w') as f:
            json.dump(job, f, default=str)

        return {'ok': True, 'data': {
            'job_id': job_id,
            'pid': proc.pid,
            'total_tasks': len(tasks),
            'total_gb': round(sum(t.get('size_gb', 0) for t in tasks), 2),
            'cell_lines': list(set(t['cell_line'] for t in tasks)),
        }}

    elif action == 'list_download_jobs':
        """List all download jobs (running, done, cancelled, error)."""
        jobs_dir = Path(os.getenv("DATA_DIR", "./data")) / "jobs"
        jobs = []
        if jobs_dir.exists():
            for jf in sorted(jobs_dir.glob("*.json")):
                if '_tasks' in jf.name:
                    continue
                try:
                    with open(jf, 'r') as f:
                        job = json.load(f)
                    # Check if process is still alive
                    pid = job.get('pid')
                    if pid and job.get('status') == 'running':
                        try:
                            os.kill(pid, 0)  # signal 0 = check if alive
                        except OSError:
                            job['status'] = 'crashed'
                    jobs.append(job)
                except Exception:
                    pass
        return {'ok': True, 'data': {'jobs': jobs}}

    elif action == 'get_job_status':
        """Get detailed status of a specific download job."""
        job_id = params.get('job_id', '')
        if not job_id:
            return {'ok': False, 'error': 'job_id is required'}
        job_file = Path(os.getenv("DATA_DIR", "./data")) / "jobs" / f"{job_id}.json"
        if not job_file.exists():
            return {'ok': False, 'error': f'Job not found: {job_id}'}
        with open(job_file, 'r') as f:
            job = json.load(f)
        # Check alive
        pid = job.get('pid')
        if pid and job.get('status') == 'running':
            try:
                os.kill(pid, 0)
            except OSError:
                job['status'] = 'crashed'
        # Read last N lines of log
        log_file = job.get('log_file', '')
        log_tail = ''
        if log_file and Path(log_file).exists():
            try:
                with open(log_file, 'r') as f:
                    lines = f.readlines()
                    log_tail = ''.join(lines[-20:])
            except Exception:
                pass
        job['log_tail'] = log_tail
        return {'ok': True, 'data': job}

    elif action == 'stop_download_job':
        """Stop a running download job."""
        import signal as sig
        job_id = params.get('job_id', '')
        if not job_id:
            return {'ok': False, 'error': 'job_id is required'}
        job_file = Path(os.getenv("DATA_DIR", "./data")) / "jobs" / f"{job_id}.json"
        if not job_file.exists():
            return {'ok': False, 'error': f'Job not found: {job_id}'}
        with open(job_file, 'r') as f:
            job = json.load(f)
        pid = job.get('pid')
        if not pid:
            return {'ok': False, 'error': 'No PID found for job'}
        try:
            os.kill(pid, sig.SIGTERM)
            job['status'] = 'stopping'
            with open(str(job_file), 'w') as f:
                json.dump(job, f, default=str)
            return {'ok': True, 'data': {'message': f'Sent SIGTERM to PID {pid}'}}
        except ProcessLookupError:
            job['status'] = 'stopped'
            with open(str(job_file), 'w') as f:
                json.dump(job, f, default=str)
            return {'ok': True, 'data': {'message': 'Process already stopped'}}
        except Exception as e:
            return {'ok': False, 'error': str(e)}

    elif action == 'delete_download_job':
        """Delete a completed/failed job record."""
        job_id = params.get('job_id', '')
        if not job_id:
            return {'ok': False, 'error': 'job_id is required'}
        jobs_dir = Path(os.getenv("DATA_DIR", "./data")) / "jobs"
        job_file = jobs_dir / f"{job_id}.json"
        log_file = jobs_dir / f"{job_id}.log"
        tasks_file = jobs_dir / f"{job_id}_tasks.json"
        if job_file.exists():
            # Don't delete running jobs
            with open(job_file, 'r') as f:
                job = json.load(f)
            if job.get('status') == 'running':
                return {'ok': False, 'error': 'Cannot delete a running job. Stop it first.'}
            job_file.unlink(missing_ok=True)
        log_file.unlink(missing_ok=True)
        tasks_file.unlink(missing_ok=True)
        return {'ok': True, 'data': {'message': f'Job {job_id} deleted'}}

    elif action == 'validate_datasets':
        """Validate all datasets for ML training readiness."""
        species = params.get('species')
        results = manager.validate_datasets(species_filter=species)
        return {'ok': True, 'data': results}

    elif action == 'test_download':
        """Test download: fetch one small cCRE file and verify it."""
        test_cell = "K562"
        ccre = manager.fetch_ccre_for_cell_line(test_cell)
        if not ccre:
            return {'ok': False, 'error': f'Could not find cCRE data for {test_cell}'}

        cache_dir = Path(os.getenv("DATA_DIR", "./data")) / "cache"
        test_path = cache_dir / f"test_{ccre['accession']}.bed.gz"
        result = manager.download_file(
            ccre['download_url'], test_path,
            accession=ccre['accession'], resume=False
        )
        if result is None:
            return {'ok': False, 'error': 'Download failed'}

        validation = manager.validate_bed_gz(str(test_path))
        actual_size = test_path.stat().st_size

        # Clean up
        test_path.unlink(missing_ok=True)

        return {'ok': True, 'data': {
            'cell_line': test_cell,
            'accession': ccre['accession'],
            'expected_size': ccre['file_size'],
            'actual_size': actual_size,
            'validation': validation,
        }}

    elif action == 'get_config':
        """Read current .env config."""
        env_path = Path(__file__).resolve().parent.parent.parent / '.env'
        config = {}
        if env_path.exists():
            with open(env_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        k, v = line.split('=', 1)
                        config[k.strip()] = v.strip()
        return {'ok': True, 'data': {
            'config': config,
            'path': str(env_path),
        }}

    elif action == 'update_config':
        """Update a config key in .env file."""
        key = params.get('key', '')
        value = params.get('value', '')
        if not key:
            return {'ok': False, 'error': 'key is required'}
        env_path = Path(__file__).resolve().parent.parent.parent / '.env'
        lines = []
        found = False
        if env_path.exists():
            with open(env_path, 'r') as f:
                lines = f.readlines()
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped and not stripped.startswith('#') and '=' in stripped:
                k = stripped.split('=', 1)[0].strip()
                if k == key:
                    lines[i] = f'{key}={value}\n'
                    found = True
                    break
        if not found:
            lines.append(f'\n{key}={value}\n')
        with open(env_path, 'w') as f:
            f.writelines(lines)
        # Reload env
        os.environ[key] = value
        return {'ok': True, 'data': {'message': f'{key} updated', 'key': key, 'value': value}}

    elif action == 'export_metadata':
        """Export all metadata to a JSON file for training pipeline consumption.
        Includes cell lines, replicates, treatments, assemblies, cCRE info, species."""
        try:
            data_dir = Path(os.getenv("DATA_DIR", "./data"))
            out_path = data_dir / "metadata" / "training_metadata.json"
            out_path.parent.mkdir(parents=True, exist_ok=True)

            # Gather all metadata
            cell_lines = manager.con.execute("""
                SELECT cl.cell_line_id, cl.cell_line_normalized as cell_line,
                       cl.tissue_type, cl.species, cl.genome_assembly,
                       cl.biosample_type, cl.has_ccre, cl.has_mcool
                FROM cell_lines cl ORDER BY cl.cell_line_normalized
            """).fetchdf().to_dict(orient='records')

            experiments = manager.con.execute("""
                SELECT ce.accession, ce.cell_line_name, ce.download_url,
                       ce.file_size_gb, ce.file_size_bytes, ce.download_status,
                       ce.species, ce.genome_assembly, ce.treatment,
                       ce.treatment_duration, ce.modification, ce.condition,
                       ce.biosample_type, ce.study, ce.dataset_label,
                       cl.cell_line_normalized
                FROM chromatin_experiments ce
                JOIN cell_lines cl ON ce.cell_line_id = cl.cell_line_id
                ORDER BY cl.cell_line_normalized, ce.accession
            """).fetchdf().to_dict(orient='records')

            ccre = manager.con.execute("""
                SELECT ra.accession, ra.download_url, ra.file_size_mb,
                       ra.assembly, ra.output_type, ra.download_status,
                       cl.cell_line_normalized as cell_line
                FROM regulatory_annotations ra
                JOIN cell_lines cl ON ra.cell_line_id = cl.cell_line_id
                ORDER BY cl.cell_line_normalized
            """).fetchdf().to_dict(orient='records')

            metadata = {
                'exported_at': __import__('datetime').datetime.now().isoformat(),
                'total_cell_lines': len(cell_lines),
                'total_experiments': len(experiments),
                'total_ccre': len(ccre),
                'cell_lines': cell_lines,
                'experiments': experiments,
                'ccre_annotations': ccre,
            }

            with open(out_path, 'w') as f:
                json.dump(metadata, f, indent=2, default=str)

            return {'ok': True, 'data': {
                'path': str(out_path),
                'cell_lines': len(cell_lines),
                'experiments': len(experiments),
                'ccre': len(ccre),
            }}
        except Exception as e:
            return {'ok': False, 'error': str(e)}

    else:
        return {'ok': False, 'error': f'Unknown action: {action}'}


def main():
    """Main IPC loop: read JSON from stdin, write JSON to stdout."""
    con = db.get_connection()
    manager = DataManager(verbose=True, use_cache=True, db_con=con)

    # If arguments passed, treat as single command
    if len(sys.argv) > 1:
        action = sys.argv[1]
        params = {}
        if len(sys.argv) > 2:
            try:
                params = json.loads(sys.argv[2])
            except json.JSONDecodeError:
                # Try key=value pairs
                for arg in sys.argv[2:]:
                    if '=' in arg:
                        k, v = arg.split('=', 1)
                        params[k] = v

        try:
            result = handle_action(action, params, manager)
            print(json.dumps(result, default=str))
        except Exception as e:
            print(json.dumps({'ok': False, 'error': str(e), 'traceback': traceback.format_exc()}))
        return

    # Interactive IPC mode: read lines from stdin
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            cmd = json.loads(line)
            action = cmd.get('action', '')
            params = cmd.get('params', {})
            result = handle_action(action, params, manager)
            print(json.dumps(result, default=str), flush=True)
        except json.JSONDecodeError as e:
            print(json.dumps({'ok': False, 'error': f'Invalid JSON: {e}'}), flush=True)
        except Exception as e:
            print(json.dumps({'ok': False, 'error': str(e), 'traceback': traceback.format_exc()}), flush=True)


if __name__ == '__main__':
    main()
