import React, { useState } from 'react';
import { Box, Text, useInput } from 'ink';
import TextInput from 'ink-text-input';
import { theme, icons } from '../theme/colors.js';
import { usePython } from '../hooks/use-python.js';
import BoxPanel from './BoxPanel.js';
import Spinner from './Spinner.js';

interface QueryRunnerProps {
  onBack: () => void;
}

const EXAMPLE_QUERIES = [
  "SELECT * FROM paired_datasets ORDER BY total_size_gb DESC LIMIT 10",
  "SELECT cell_line_name, COUNT(*) as reps FROM chromatin_experiments GROUP BY cell_line_name ORDER BY reps DESC",
  "SELECT * FROM stats_summary",
  "SELECT tissue_type, COUNT(*) as n FROM cell_lines GROUP BY tissue_type ORDER BY n DESC",
  "SELECT SUM(file_size_gb) as total_gb FROM chromatin_experiments",
  "SELECT * FROM cell_lines WHERE has_ccre = TRUE AND has_mcool = TRUE",
];

export default function QueryRunner({ onBack }: QueryRunnerProps) {
  const { data, loading, error, call } = usePython();
  const [query, setQuery] = useState('');
  const [history, setHistory] = useState<string[]>([]);
  const [exampleIdx, setExampleIdx] = useState(0);

  useInput((input, key) => {
    if (key.escape) {
      onBack();
      return;
    }
    if (key.tab) {
      // Cycle through examples
      setQuery(EXAMPLE_QUERIES[exampleIdx % EXAMPLE_QUERIES.length]!);
      setExampleIdx(prev => prev + 1);
    }
  });

  function handleSubmit(value: string) {
    if (!value.trim()) return;
    setHistory(prev => [...prev, value]);
    call('query', { sql: value });
  }

  const rows = data?.rows || [];
  const columns = rows.length > 0 ? Object.keys(rows[0]) : [];

  return (
    <Box flexDirection="column">
      <BoxPanel title={`${icons.database} SQL Query Runner (DuckDB)`} titleColor={theme.blue}>
        <Box marginBottom={1}>
          <Text color={theme.muted}>Tables: </Text>
          <Text color={theme.cyan}>cell_lines</Text>
          <Text color={theme.muted}>, </Text>
          <Text color={theme.cyan}>chromatin_experiments</Text>
          <Text color={theme.muted}>, </Text>
          <Text color={theme.cyan}>regulatory_annotations</Text>
          <Text color={theme.muted}>, </Text>
          <Text color={theme.cyan}>paired_datasets</Text>
          <Text color={theme.muted}>, </Text>
          <Text color={theme.cyan}>stats_summary</Text>
        </Box>

        <Box>
          <Text color={theme.green} bold>{'> '}</Text>
          <TextInput
            value={query}
            onChange={setQuery}
            onSubmit={handleSubmit}
            placeholder="Enter SQL query (Tab for examples, Esc to go back)"
          />
        </Box>

        <Text color={theme.muted} dimColor>Tab to cycle examples {icons.dot} Enter to execute {icons.dot} Esc to go back</Text>
      </BoxPanel>

      {loading && <Spinner label="Executing query..." />}

      {error && (
        <BoxPanel title="Error" titleColor={theme.error}>
          <Text color={theme.error}>{error}</Text>
        </BoxPanel>
      )}

      {rows.length > 0 && (
        <BoxPanel title={`Results (${data?.count || rows.length} rows)`} titleColor={theme.green}>
          {/* Column headers */}
          <Box>
            {columns.map((col, i) => (
              <Text key={i} color={theme.blue} bold>
                {padR(col, Math.max(col.length + 2, 15))}
              </Text>
            ))}
          </Box>
          <Text color={theme.border}>{'─'.repeat(columns.length * 15)}</Text>

          {/* Rows */}
          {rows.slice(0, 25).map((row: any, i: number) => (
            <Box key={i}>
              {columns.map((col, j) => {
                const val = row[col];
                const display = val === null ? 'NULL' : typeof val === 'number'
                  ? (Number.isInteger(val) ? String(val) : val.toFixed(2))
                  : String(val);
                return (
                  <Text key={j} color={typeof val === 'number' ? theme.orange : theme.fg}>
                    {padR(display, Math.max(col.length + 2, 15))}
                  </Text>
                );
              })}
            </Box>
          ))}

          {rows.length > 25 && (
            <Text color={theme.muted}>  ... {rows.length - 25} more rows</Text>
          )}
        </BoxPanel>
      )}

      {data && rows.length === 0 && !loading && !error && (
        <BoxPanel title="Results" titleColor={theme.muted}>
          <Text color={theme.muted}>No rows returned.</Text>
        </BoxPanel>
      )}
    </Box>
  );
}

function padR(s: string, n: number): string {
  return s.length >= n ? s.slice(0, n) : s + ' '.repeat(n - s.length);
}
