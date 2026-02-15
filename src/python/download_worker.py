#!/usr/bin/env python3
"""
Background Download Worker
============================
Runs as a detached nohup process to download datasets in the background.
Does NOT use DuckDB — all task info (URLs, paths) is pre-resolved by the
IPC layer and passed as a JSON task list. This avoids DuckDB's single-writer lock.

Writes progress to a JSON job file that the UI can poll.

Usage:
  python3 download_worker.py <job_id> <job_file> <tasks_json_file>
"""

import sys
import json
import os
import signal
import requests
from pathlib import Path
from datetime import datetime

# Load .env from project root if available
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent.parent / '.env')
except ImportError:
    pass

FOURDN_ACCESS_ID = os.getenv('FOURDN_ACCESS_ID', '')
FOURDN_SECRET_KEY = os.getenv('FOURDN_SECRET_KEY', '')


def update_job(job_file, updates):
    """Atomically update the job status file."""
    try:
        if Path(job_file).exists():
            with open(job_file, 'r') as f:
                job = json.load(f)
        else:
            job = {}
        job.update(updates)
        job['updated_at'] = datetime.now().isoformat()
        tmp = job_file + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(job, f, default=str)
        os.replace(tmp, job_file)
    except Exception:
        pass


def download_file(url, out_path, file_type='mcool', resume=True):
    """Download a file with resume support. Uses 4DN auth for mcool files.
    Returns path on success, None on failure."""
    if not url:
        return None
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    headers = {}
    mode = 'wb'
    existing = 0
    if resume and out.exists():
        existing = out.stat().st_size
        if existing > 0:
            headers['Range'] = f'bytes={existing}-'
            mode = 'ab'

    # Use 4DN auth for mcool files from data.4dnucleome.org
    auth = None
    if '4dnucleome.org' in (url or '') and FOURDN_ACCESS_ID and FOURDN_SECRET_KEY:
        auth = (FOURDN_ACCESS_ID, FOURDN_SECRET_KEY)

    try:
        resp = requests.get(url, headers=headers, auth=auth, stream=True,
                            timeout=120, allow_redirects=True)
        if resp.status_code == 416:
            # Already complete
            return str(out)
        resp.raise_for_status()

        with open(out, mode) as f:
            for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                if chunk:
                    f.write(chunk)
        return str(out)
    except Exception as e:
        print(f"Download error for {url}: {e}", file=sys.stderr)
        return None


def main():
    if len(sys.argv) < 4:
        print("Usage: download_worker.py <job_id> <job_file> <tasks_json_file>", file=sys.stderr)
        sys.exit(1)

    job_id = sys.argv[1]
    job_file = sys.argv[2]
    tasks_file = sys.argv[3]

    # Read pre-resolved tasks from file
    try:
        with open(tasks_file, 'r') as f:
            tasks = json.load(f)
    except Exception as e:
        update_job(job_file, {'status': 'error', 'error': f'Failed to read tasks: {e}'})
        sys.exit(1)

    total_tasks = len(tasks)
    total_gb = sum(t.get('size_gb', 0) for t in tasks)

    # Handle SIGTERM gracefully
    cancelled = [False]
    def handle_signal(sig, frame):
        cancelled[0] = True
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    # Initialize job file
    update_job(job_file, {
        'job_id': job_id,
        'status': 'running',
        'pid': os.getpid(),
        'started_at': datetime.now().isoformat(),
        'total_tasks': total_tasks,
        'total_gb': round(total_gb, 2),
        'completed': 0,
        'failed': 0,
        'current': '',
        'results': [],
        'cell_lines': list(set(t.get('cell_line', '') for t in tasks)),
    })

    results = []
    completed = 0
    failed = 0

    for i, task in enumerate(tasks):
        if cancelled[0]:
            update_job(job_file, {
                'status': 'cancelled',
                'completed': completed,
                'failed': failed,
                'results': results,
            })
            sys.exit(0)

        update_job(job_file, {
            'current': f"[{i+1}/{total_tasks}] {task.get('type','')} {task.get('cell_line','')} ({task.get('accession','')})",
            'current_idx': i,
            'completed': completed,
            'failed': failed,
        })

        try:
            result_path = download_file(
                task.get('url', ''),
                task.get('path', ''),
                file_type=task.get('type', 'mcool'),
                resume=True,
            )
            success = result_path is not None
            if success:
                completed += 1
            else:
                failed += 1
            results.append({
                'type': task.get('type', ''),
                'cell_line': task.get('cell_line', ''),
                'accession': task.get('accession', ''),
                'success': success,
                'path': result_path,
                'size_gb': task.get('size_gb', 0),
            })
        except Exception as e:
            failed += 1
            results.append({
                'type': task.get('type', ''),
                'cell_line': task.get('cell_line', ''),
                'accession': task.get('accession', ''),
                'success': False,
                'error': str(e),
            })

        # Update progress after each file
        update_job(job_file, {
            'completed': completed,
            'failed': failed,
            'results': results,
        })

    # Done
    update_job(job_file, {
        'status': 'done',
        'completed': completed,
        'failed': failed,
        'finished_at': datetime.now().isoformat(),
        'current': '',
        'results': results,
    })


if __name__ == '__main__':
    main()
