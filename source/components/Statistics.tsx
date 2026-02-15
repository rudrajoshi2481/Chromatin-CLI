import React, { useEffect } from 'react';
import { Box, Text, useInput } from 'ink';
import { theme, icons } from '../theme/colors.js';
import { usePython } from '../hooks/use-python.js';
import { useTerminalSize } from '../hooks/use-terminal-size.js';
import BoxPanel from './BoxPanel.js';
import Spinner from './Spinner.js';
import { gbFormat, mbFormat, barChart, sparkline } from '../lib/format.js';

interface StatisticsProps {
  onBack: () => void;
}

export default function Statistics({ onBack }: StatisticsProps) {
  const { data, loading, error, call } = usePython();
  const { columns: termCols } = useTerminalSize();
  const sepWidth = Math.min(70, termCols - 6);

  useEffect(() => {
    call('get_stats');
  }, []);

  useInput((input, key) => {
    if (key.escape || input === 'q') {
      onBack();
    }
  });

  if (loading) {
    return <Spinner label="Computing statistics..." />;
  }

  if (error) {
    return (
      <Box flexDirection="column">
        <Text color={theme.error}>{icons.cross} {error}</Text>
        <Text color={theme.muted}>Press q to go back</Text>
      </Box>
    );
  }

  const summary = data?.summary || {};
  const cellLines = data?.cell_lines || [];
  const tissues = data?.tissues || [];
  const sizeDist = data?.size_distribution || [];
  const paired = data?.paired || [];
  const dlStatus = data?.download_status || [];

  const isEmpty = (summary.total_cell_lines || 0) === 0;

  if (isEmpty) {
    return (
      <BoxPanel title={`${icons.chart} Statistics`} titleColor={theme.blue}>
        <Text color={theme.warning}>No data available. Run --fetch first.</Text>
        <Text color={theme.muted}>Press q to go back</Text>
      </BoxPanel>
    );
  }

  // Compute derived stats
  const replicateCounts = cellLines.map((cl: any) => cl.replicates || 0);
  const avgReplicates = replicateCounts.length > 0
    ? (replicateCounts.reduce((a: number, b: number) => a + b, 0) / replicateCounts.length).toFixed(1)
    : '0';
  const maxReplicates = Math.max(0, ...replicateCounts);
  const pairedCount = cellLines.filter((cl: any) => cl.has_ccre && cl.has_mcool).length;
  const unpaired = cellLines.length - pairedCount;

  return (
    <Box flexDirection="column">
      {/* Overview */}
      <BoxPanel title={`${icons.chart} Data Overview`} titleColor={theme.blue}>
        <Box>
          <Box flexDirection="column" marginRight={4} width={35}>
            <StatRow label="Total Cell Lines" value={String(summary.total_cell_lines || 0)} color={theme.white} />
            <StatRow label="Paired (Hi-C + cCRE)" value={String(pairedCount)} color={theme.green} />
            <StatRow label="Unpaired" value={String(unpaired)} color={theme.warning} />
            <StatRow label="Avg Replicates" value={avgReplicates} color={theme.cyan} />
            <StatRow label="Max Replicates" value={String(maxReplicates)} color={theme.orange} />
          </Box>
          <Box flexDirection="column" width={35}>
            <StatRow label="Chromatin Files" value={String(summary.total_chromatin || 0)} color={theme.cyan} />
            <StatRow label="cCRE Files" value={String(summary.total_ccre || 0)} color={theme.purple} />
            <StatRow label="Total Chromatin" value={gbFormat(summary.total_chromatin_gb || 0)} color={theme.orange} />
            <StatRow label="Total cCRE" value={mbFormat(summary.total_ccre_mb || 0)} color={theme.teal} />
            <StatRow label="Downloaded" value={`${summary.downloaded_chromatin || 0} chr + ${summary.downloaded_ccre || 0} cCRE`} color={theme.success} />
          </Box>
        </Box>
      </BoxPanel>

      {/* Replicate Distribution (horizontal bar chart) */}
      {cellLines.length > 0 && (
        <BoxPanel title={`${icons.dna} Replicates per Cell Line (Top 15)`} titleColor={theme.purple}>
          {cellLines.slice(0, 15).map((cl: any, i: number) => {
            const maxRep = Math.max(1, ...cellLines.slice(0, 15).map((c: any) => c.replicates || 0));
            return (
              <Box key={i}>
                <Text color={theme.fg}>{padR(cl.cell_line || '', 20)}</Text>
                <Text color={theme.blue}>{barChart(cl.replicates || 0, maxRep, 30)} </Text>
                <Text color={theme.white} bold>{cl.replicates || 0}</Text>
              </Box>
            );
          })}
          {cellLines.length > 15 && (
            <Text color={theme.muted}>  ... {cellLines.length - 15} more cell lines</Text>
          )}
        </BoxPanel>
      )}

      {/* Tissue Distribution */}
      {tissues.length > 0 && (
        <BoxPanel title={`${icons.search} Tissue Type Coverage`} titleColor={theme.teal}>
          {tissues.map((t: any, i: number) => {
            const maxT = Math.max(1, ...tissues.map((x: any) => x.count));
            return (
              <Box key={i}>
                <Text color={theme.fg}>{padR(t.tissue || '', 30)}</Text>
                <Text color={theme.teal}>{barChart(t.count, maxT, 25)} </Text>
                <Text color={theme.white}>{t.count} cell lines</Text>
              </Box>
            );
          })}
        </BoxPanel>
      )}

      {/* File Size Distribution */}
      {sizeDist.length > 0 && (
        <BoxPanel title={`${icons.folder} File Size Distribution`} titleColor={theme.orange}>
          {sizeDist.map((s: any, i: number) => {
            const maxS = Math.max(1, ...sizeDist.map((x: any) => x.count));
            return (
              <Box key={i}>
                <Text color={theme.fg}>{padR(s.size_range || '', 15)}</Text>
                <Text color={theme.orange}>{barChart(s.count, maxS, 30)} </Text>
                <Text color={theme.white}>{s.count} files</Text>
              </Box>
            );
          })}
        </BoxPanel>
      )}

      {/* Download Status */}
      {dlStatus.length > 0 && (
        <BoxPanel title={`${icons.download} Download Status`} titleColor={theme.cyan}>
          {dlStatus.map((d: any, i: number) => (
            <Box key={i}>
              <Text color={d.status === 'downloaded' ? theme.success : d.status === 'failed' ? theme.error : theme.warning}>
                {d.status === 'downloaded' ? icons.check : d.status === 'failed' ? icons.cross : icons.circle}
              </Text>
              <Text color={theme.fg}> {padR(d.status || '', 15)}</Text>
              <Text color={theme.white}>{d.count}</Text>
            </Box>
          ))}
        </BoxPanel>
      )}

      {/* Paired Datasets Summary */}
      {paired.length > 0 && (
        <BoxPanel title={`${icons.star} Paired Dataset Summary`} titleColor={theme.green}>
          <Box>
            <Text color={theme.blue} bold>{padR('Cell Line', 22)}</Text>
            <Text color={theme.blue} bold>{padR('Tissue', 25)}</Text>
            <Text color={theme.blue} bold>{padR('Chr', 5)}</Text>
            <Text color={theme.blue} bold>{padR('cCRE', 5)}</Text>
            <Text color={theme.blue} bold>{padR('Total GB', 10)}</Text>
          </Box>
          <Text color={theme.border}>{'─'.repeat(sepWidth)}</Text>
          {paired.slice(0, 12).map((p: any, i: number) => (
            <Box key={i}>
              <Text color={theme.white}>{padR(p.cell_line_name || '', 22)}</Text>
              <Text color={theme.muted}>{padR(p.tissue_type || '', 25)}</Text>
              <Text color={theme.cyan}>{padR(String(p.chromatin_count || 0), 5)}</Text>
              <Text color={theme.purple}>{padR(String(p.ccre_count || 0), 5)}</Text>
              <Text color={theme.orange}>{padR(gbFormat(p.total_size_gb || 0), 10)}</Text>
            </Box>
          ))}
          {paired.length > 12 && (
            <Text color={theme.muted}>  ... and {paired.length - 12} more</Text>
          )}
        </BoxPanel>
      )}

      {/* Sparkline */}
      {cellLines.length > 3 && (
        <BoxPanel title="Replicate Trend (sparkline)" titleColor={theme.muted}>
          <Text color={theme.cyan}>
            {sparkline(cellLines.slice(0, 40).map((cl: any) => cl.replicates || 0))}
          </Text>
        </BoxPanel>
      )}

      <Box>
        <Text color={theme.muted}>Press </Text>
        <Text color={theme.cyan}>q</Text>
        <Text color={theme.muted}> to go back</Text>
      </Box>
    </Box>
  );
}

function StatRow({ label, value, color }: { label: string; value: string; color: string }) {
  return (
    <Box>
      <Text color={theme.muted}>{padR(label + ':', 25)}</Text>
      <Text color={color} bold>{value}</Text>
    </Box>
  );
}

function padR(s: string, n: number): string {
  return s.length >= n ? s.slice(0, n) : s + ' '.repeat(n - s.length);
}
