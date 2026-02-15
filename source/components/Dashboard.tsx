import React, { useEffect, useState } from 'react';
import { Box, Text, useInput } from 'ink';
import { theme, icons } from '../theme/colors.js';
import { usePython } from '../hooks/use-python.js';
import { useTerminalSize } from '../hooks/use-terminal-size.js';
import BoxPanel from './BoxPanel.js';
import Spinner from './Spinner.js';
import { gbFormat, mbFormat, barChart } from '../lib/format.js';

interface DashboardProps {
  onNavigate: (view: string) => void;
}

export default function Dashboard({ onNavigate }: DashboardProps) {
  const { data, loading, error, call } = usePython();
  const [page, setPage] = useState(0);

  const { columns: termCols, rows: termRows } = useTerminalSize();
  const pageSize = Math.max(8, termRows - 28); // reserve for panels above
  const sepWidth = Math.min(82, termCols - 6);

  useEffect(() => {
    call('get_stats');
  }, []);

  useInput((input, key) => {
    if (key.escape || input === 'q') {
      onNavigate('menu');
      return;
    }
    if (key.rightArrow || input === 'n') {
      setPage(prev => prev + 1);
    }
    if (key.leftArrow || input === 'p') {
      setPage(prev => Math.max(0, prev - 1));
    }
  });

  if (loading) {
    return <Spinner label="Loading database statistics..." />;
  }

  if (error) {
    return (
      <Box flexDirection="column">
        <Text color={theme.error}>{icons.cross} Error: {error}</Text>
        <Text color={theme.muted}>Run with --fetch first to populate the database.</Text>
        <Text color={theme.muted}>Press q to go back</Text>
      </Box>
    );
  }

  const summary = data?.summary || {};
  const cellLines = data?.cell_lines || [];
  const tissues = data?.tissues || [];
  const speciesList: any[] = data?.species || [];
  const treatments: any[] = data?.treatments || [];

  const isEmpty = (summary.total_cell_lines || 0) === 0;
  const totalPages = Math.max(1, Math.ceil(cellLines.length / pageSize));
  const currentPage = Math.min(page, totalPages - 1);
  const pageItems = cellLines.slice(currentPage * pageSize, (currentPage + 1) * pageSize);

  // ML readiness counts
  const mlReady = cellLines.filter((c: any) => c.has_ccre && c.has_mcool).length;
  const humanReady = cellLines.filter((c: any) => c.has_ccre && c.has_mcool && (c.species || '') === 'Homo sapiens').length;

  return (
    <Box flexDirection="column" width={termCols}>
      {/* Summary + Species side by side */}
      <Box flexWrap="wrap">
        <Box flexDirection="column" flexGrow={1} marginRight={1} minWidth={40}>
          <BoxPanel title={`${icons.chart} Database Summary`} titleColor={theme.blue}>
            {isEmpty ? (
              <Box flexDirection="column">
                <Text color={theme.warning}>No data in database yet.</Text>
                <Text color={theme.muted}>Press <Text color={theme.cyan}>6</Text> from menu or run: <Text color={theme.green}>chromatin-manager --fetch</Text></Text>
              </Box>
            ) : (
              <Box>
                <Box flexDirection="column" marginRight={3}>
                  <StatLine label="Cell Lines" value={summary.total_cell_lines || 0} color={theme.white} />
                  <StatLine label="Paired (ML-ready)" value={`${mlReady} (${humanReady} human)`} color={theme.green} />
                  <StatLine label="Chromatin Files" value={summary.total_chromatin || 0} color={theme.cyan} />
                  <StatLine label="cCRE Files" value={summary.total_ccre || 0} color={theme.purple} />
                </Box>
                <Box flexDirection="column">
                  <StatLine label="Total Chromatin" value={gbFormat(summary.total_chromatin_gb || 0)} color={theme.orange} />
                  <StatLine label="Total cCRE" value={mbFormat(summary.total_ccre_mb || 0)} color={theme.teal} />
                  <StatLine label="Downloaded Chr" value={summary.downloaded_chromatin || 0} color={theme.success} />
                  <StatLine label="Downloaded cCRE" value={summary.downloaded_ccre || 0} color={theme.success} />
                </Box>
              </Box>
            )}
          </BoxPanel>
        </Box>

        {/* Species Breakdown */}
        {speciesList.length > 0 && (
          <Box flexDirection="column" width={36}>
            <BoxPanel title={`${icons.dna} Species`} titleColor={theme.teal}>
              {speciesList.map((sp: any, i: number) => {
                const name = (sp.species || 'Unknown');
                const short = name.includes('sapiens') ? 'Human'
                  : name.includes('musculus') ? 'Mouse'
                  : name.includes('rerio') ? 'Zebrafish' : name.slice(0, 12);
                const color = short === 'Human' ? theme.cyan
                  : short === 'Mouse' ? theme.orange : theme.purple;
                return (
                  <Box key={i}>
                    <Text color={color}>{padR(short, 12)}</Text>
                    <Text color={theme.muted}>{padR(String(sp.cell_line_count || 0) + ' cls', 8)}</Text>
                    <Text color={theme.white}>{sp.experiment_count || 0} exp</Text>
                    <Text color={theme.success}> {sp.with_ccre || 0} cCRE</Text>
                  </Box>
                );
              })}
            </BoxPanel>
          </Box>
        )}
      </Box>

      {/* Treatment + Tissue side by side */}
      <Box flexWrap="wrap">
        {/* Treatment Stats */}
        {treatments.length > 0 && (
          <Box flexDirection="column" flexGrow={1} marginRight={1} minWidth={40}>
            <BoxPanel title={`${icons.search} Treatment Conditions`} titleColor={theme.orange}>
              {treatments.slice(0, 6).map((t: any, i: number) => {
                const label = (t.treatment || 'untreated');
                const isUntreated = label === 'untreated';
                return (
                  <Box key={i}>
                    <Text color={isUntreated ? theme.green : theme.warning}>
                      {isUntreated ? icons.check : icons.circle}
                    </Text>
                    <Text color={theme.muted}> {padR(label.slice(0, 40), 42)}</Text>
                    <Text color={theme.white}>{t.count} files</Text>
                    <Text color={theme.muted}> ({t.cell_lines} cls)</Text>
                  </Box>
                );
              })}
              {treatments.length > 6 && (
                <Text color={theme.muted}>  ... {treatments.length - 6} more conditions</Text>
              )}
            </BoxPanel>
          </Box>
        )}

        {/* Tissue Distribution */}
        {tissues.length > 0 && (
          <Box flexDirection="column" width={40}>
            <BoxPanel title={`${icons.star} Tissues`} titleColor={theme.purple}>
              {tissues.slice(0, 6).map((t: any, i: number) => {
                const maxCount = Math.max(...tissues.map((x: any) => x.count));
                return (
                  <Box key={i}>
                    <Text color={theme.muted}>{padR(t.tissue, 22)}</Text>
                    <Text color={theme.blue}>{barChart(t.count, maxCount, 10)} </Text>
                    <Text color={theme.white}>{t.count}</Text>
                  </Box>
                );
              })}
            </BoxPanel>
          </Box>
        )}
      </Box>

      {/* Cell Lines Table with Pagination */}
      {cellLines.length > 0 && (
        <BoxPanel title={`${icons.dna} Cell Lines — Page ${currentPage + 1}/${totalPages}`} titleColor={theme.teal}>
          <Box>
            <Text color={theme.blue} bold>{padR('Cell Line', 20)}</Text>
            <Text color={theme.blue} bold>{padR('Species', 8)}</Text>
            <Text color={theme.blue} bold>{padR('Tissue', 20)}</Text>
            <Text color={theme.blue} bold>{padR('Reps', 5)}</Text>
            <Text color={theme.blue} bold>{padR('Size', 10)}</Text>
            <Text color={theme.blue} bold>{padR('cCRE', 5)}</Text>
            <Text color={theme.blue} bold>{padR('Asm', 8)}</Text>
            <Text color={theme.blue} bold>ML</Text>
          </Box>
          <Text color={theme.border}>{'─'.repeat(sepWidth)}</Text>
          {pageItems.map((cl: any, i: number) => {
            const hasPair = cl.has_ccre && cl.has_mcool;
            const sp = (cl.species || '').includes('sapiens') ? 'Hum'
              : (cl.species || '').includes('musculus') ? 'Mus'
              : (cl.species || '').includes('rerio') ? 'Zeb' : '?';
            const spColor = sp === 'Hum' ? theme.cyan : sp === 'Mus' ? theme.orange : theme.purple;
            return (
              <Box key={i}>
                <Text color={theme.white}>{padR(cl.cell_line || '', 20)}</Text>
                <Text color={spColor}>{padR(sp, 8)}</Text>
                <Text color={theme.muted}>{padR(cl.tissue || '', 20)}</Text>
                <Text color={theme.orange}>{padR(String(cl.replicates || 0), 5)}</Text>
                <Text color={theme.cyan}>{padR(gbFormat(cl.total_gb || 0), 10)}</Text>
                <Text color={cl.has_ccre ? theme.success : theme.error}>
                  {padR(cl.has_ccre ? icons.check : icons.cross, 5)}
                </Text>
                <Text color={theme.muted}>{padR(cl.genome_assembly || '?', 8)}</Text>
                <Text color={hasPair ? theme.success : theme.warning}>
                  {hasPair ? 'Ready' : 'No'}
                </Text>
              </Box>
            );
          })}
          <Text color={theme.border}>{'─'.repeat(sepWidth)}</Text>
          <Box>
            <Text color={theme.muted}>[</Text><Text color={theme.cyan}>←→</Text>
            <Text color={theme.muted}>] Page  [</Text><Text color={theme.cyan}>q</Text>
            <Text color={theme.muted}>] Back  </Text>
            <Text color={theme.muted}>{cellLines.length} cell lines</Text>
          </Box>
        </BoxPanel>
      )}
    </Box>
  );
}

function StatLine({ label, value, color }: { label: string; value: string | number; color: string }) {
  return (
    <Box>
      <Text color={theme.muted}>{padR(label + ':', 22)}</Text>
      <Text color={color} bold>{String(value)}</Text>
    </Box>
  );
}

function padR(s: string, n: number): string {
  return s.length >= n ? s.slice(0, n) : s + ' '.repeat(n - s.length);
}
