#!/usr/bin/env node
import React from 'react';
import { render } from 'ink';
import meow from 'meow';
import App, { type AppFlags } from './app.js';

const cli = meow(
  `
  Usage
    $ chromatin-manager [options]

  Interactive Mode (default)
    $ chromatin-manager              Launch interactive TUI

  Direct Actions
    $ chromatin-manager --fetch      Fetch data from 4DN/ENCODE APIs
    $ chromatin-manager --list       List available datasets (JSON)
    $ chromatin-manager --report     Generate CSV report
    $ chromatin-manager --test       Test download a small cCRE file
    $ chromatin-manager --duplicates Check for duplicate experiments

  Filters (combine with any action)
    --paired                Only paired datasets (Hi-C + cCRE)
    --non-paired            Only non-paired datasets
    --cell-line <name>      Filter by cell line name
    --tissues <list>        Filter by tissue types (comma-separated)
    --one-per-cell          Select one sample per cell line (largest)
    --min-replicates <n>    Minimum replicate count
    --max-replicates <n>    Maximum replicate count
    --min-size <gb>         Minimum file size in GB
    --max-size <gb>         Maximum file size in GB
    --limit <n>             Limit number of results

  Download Options
    --download              Download datasets for a cell line
    --download-all          Download all matching datasets
    --no-resume             Disable download resume
    --no-cache              Disable API cache

  Other
    --output <path>         Output file path for reports
    --verbose               Verbose output
    --help                  Show this help

  Examples
    $ chromatin-manager --fetch
    $ chromatin-manager --list --paired --cell-line GM12878
    $ chromatin-manager --download --cell-line K562 --paired
    $ chromatin-manager --download-all --paired --limit 5
    $ chromatin-manager --download-all --tissues "Leukemia,B-lymphocyte"
    $ chromatin-manager --list --min-replicates 3 --max-size 30
    $ chromatin-manager --report --one-per-cell --output report.csv
`,
  {
    importMeta: import.meta,
    flags: {
      // Actions
      fetch: { type: 'boolean', default: false },
      list: { type: 'boolean', default: false },
      report: { type: 'boolean', default: false },
      test: { type: 'boolean', default: false },
      duplicates: { type: 'boolean', default: false },
      download: { type: 'boolean', default: false },
      downloadAll: { type: 'boolean', default: false },

      // Filters
      paired: { type: 'boolean', default: false },
      nonPaired: { type: 'boolean', default: false },
      cellLine: { type: 'string' },
      tissues: { type: 'string' },
      onePerCell: { type: 'boolean', default: false },
      minReplicates: { type: 'number' },
      maxReplicates: { type: 'number' },
      minSize: { type: 'number' },
      maxSize: { type: 'number' },
      limit: { type: 'number' },

      // Options
      noResume: { type: 'boolean', default: false },
      noCache: { type: 'boolean', default: false },
      output: { type: 'string' },
      verbose: { type: 'boolean', shortFlag: 'v', default: false },
    },
  },
);

// Parse tissues from comma-separated string
const tissuesList = cli.flags.tissues
  ? cli.flags.tissues.split(',').map(t => t.trim()).filter(Boolean)
  : undefined;

// Handle non-interactive (JSON output) modes
const isNonInteractive = cli.flags.list || cli.flags.report || cli.flags.test || cli.flags.duplicates;

if (isNonInteractive) {
  // Use Python IPC directly for non-interactive modes
  import('./lib/python-bridge.js').then(async ({ pythonCall }) => {
    try {
      if (cli.flags.list) {
        const action = cli.flags.paired ? 'find_paired' : cli.flags.nonPaired ? 'find_non_paired' : 'fetch_mcool';
        const result = await pythonCall(action, {
          cell_line: cli.flags.cellLine,
          one_per_cell: cli.flags.onePerCell,
          min_replicates: cli.flags.minReplicates,
          max_replicates: cli.flags.maxReplicates,
          min_size_gb: cli.flags.minSize,
          max_size_gb: cli.flags.maxSize,
          tissues: tissuesList,
        });
        console.log(JSON.stringify(result, null, 2));
      } else if (cli.flags.report) {
        const result = await pythonCall('generate_report', {
          one_per_cell: cli.flags.onePerCell,
          output_csv: cli.flags.output,
        });
        console.log(JSON.stringify(result, null, 2));
      } else if (cli.flags.test) {
        const result = await pythonCall('test_download', {});
        console.log(JSON.stringify(result, null, 2));
      } else if (cli.flags.duplicates) {
        const result = await pythonCall('check_duplicates', {});
        console.log(JSON.stringify(result, null, 2));
      }
    } catch (e: any) {
      console.error(JSON.stringify({ ok: false, error: e.message }));
      process.exit(1);
    }
    process.exit(0);
  });
} else {
  // Interactive TUI mode
  const appFlags: AppFlags = {
    fetch: cli.flags.fetch,
    paired: cli.flags.paired,
    nonPaired: cli.flags.nonPaired,
    cellLine: cli.flags.cellLine,
    tissues: tissuesList,
    onePerCell: cli.flags.onePerCell,
    minReplicates: cli.flags.minReplicates,
    maxReplicates: cli.flags.maxReplicates,
    minSizeGb: cli.flags.minSize,
    maxSizeGb: cli.flags.maxSize,
    limit: cli.flags.limit,
    download: cli.flags.download,
    downloadAll: cli.flags.downloadAll,
    verbose: cli.flags.verbose,
    noCache: cli.flags.noCache,
    output: cli.flags.output,
  };

  render(<App flags={appFlags} />);
}
