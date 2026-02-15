import React, { useEffect, useState } from 'react';
import { Box, Text, useInput, useStdout } from 'ink';
import { theme, icons } from '../theme/colors.js';
import { usePython } from '../hooks/use-python.js';
import BoxPanel from './BoxPanel.js';
import Spinner from './Spinner.js';
import { gbFormat, mbFormat } from '../lib/format.js';

interface DownloadManagerProps {
  cellLine?: string;
  paired?: boolean;
  limit?: number;
  onePerCell?: boolean;
  tissues?: string[];
  minSizeGb?: number;
  maxSizeGb?: number;
  onBack: () => void;
}

type SpeciesFilter = 'all' | 'human' | 'mouse';

export default function DownloadManager({
  cellLine, paired = true, limit, onePerCell = true,
  tissues, minSizeGb, maxSizeGb, onBack,
}: DownloadManagerProps) {
  const finder = usePython();
  const downloader = usePython();
  const [phase, setPhase] = useState<'scanning' | 'preview' | 'downloading' | 'done'>('scanning');
  const [selectedIdx, setSelectedIdx] = useState(0);
  const [selectedItems, setSelectedItems] = useState<Set<number>>(new Set());
  const [speciesFilter, setSpeciesFilter] = useState<SpeciesFilter>('all');
  const [ccreOnly, setCcreOnly] = useState(false);
  const [page, setPage] = useState(0);

  // Dynamic page size
  const { stdout } = useStdout();
  const termRows = stdout?.rows || 40;
  const pageSize = Math.max(8, termRows - 12);

  useEffect(() => {
    const params: Record<string, any> = {};
    if (cellLine) params.cell_line = cellLine;
    if (onePerCell) params.one_per_cell = true;
    if (tissues) params.tissues = tissues;
    if (minSizeGb !== undefined) params.min_size_gb = minSizeGb;
    if (maxSizeGb !== undefined) params.max_size_gb = maxSizeGb;

    const action = paired ? 'find_paired' : 'fetch_mcool';
    finder.call(action, params).then(() => setPhase('preview'));
  }, []);

  useInput((input, key) => {
    if (key.escape || input === 'q') {
      onBack();
      return;
    }

    if (phase === 'preview') {
      const filtered = getFilteredItems();
      if (key.upArrow) setSelectedIdx(prev => Math.max(0, prev - 1));
      if (key.downArrow) setSelectedIdx(prev => Math.min(Math.min(pageSize, filtered.length) - 1, prev + 1));

      if (key.rightArrow || input === 'n') {
        setPage(prev => prev + 1);
        setSelectedIdx(0);
      }
      if (key.leftArrow || input === 'p') {
        setPage(prev => Math.max(0, prev - 1));
        setSelectedIdx(0);
      }

      if (input === ' ') {
        // Toggle selection using absolute index
        const absIdx = page * pageSize + selectedIdx;
        setSelectedItems(prev => {
          const next = new Set(prev);
          if (next.has(absIdx)) next.delete(absIdx);
          else next.add(absIdx);
          return next;
        });
      }

      if (input === 'a') {
        const filtered = getFilteredItems();
        setSelectedItems(prev => {
          if (prev.size === filtered.length) return new Set();
          return new Set(filtered.map((_: any, i: number) => i));
        });
      }

      if (input === 'd' || key.return) {
        startDownload();
      }

      // Species filter
      if (key.tab) {
        const tabs: SpeciesFilter[] = ['all', 'human', 'mouse'];
        setSpeciesFilter(prev => tabs[(tabs.indexOf(prev) + 1) % tabs.length]!);
        setPage(0);
        setSelectedIdx(0);
        setSelectedItems(new Set());
      }

      // cCRE toggle
      if (input === 'c') {
        setCcreOnly(prev => !prev);
        setPage(0);
        setSelectedIdx(0);
        setSelectedItems(new Set());
      }
    }
  });

  function getItems(): any[] {
    if (paired) {
      return finder.data?.paired || [];
    }
    return finder.data?.files || [];
  }

  function getFilteredItems(): any[] {
    let items = getItems();
    if (speciesFilter === 'human') {
      items = items.filter((it: any) => {
        const cl = (it.cell_line || '').toLowerCase();
        const sp = (it.species || '').toLowerCase();
        return sp.includes('sapiens') || (!sp.includes('musculus') && !sp.includes('rerio') &&
          !cl.includes('mouse') && !cl.includes('embryo') && !cl.includes('zebrafish'));
      });
    } else if (speciesFilter === 'mouse') {
      items = items.filter((it: any) => {
        const cl = (it.cell_line || '').toLowerCase();
        const sp = (it.species || '').toLowerCase();
        return sp.includes('musculus') || cl.includes('mouse') || cl.includes('es-e14') || cl.includes('ch12');
      });
    }
    if (ccreOnly && !paired) {
      // For non-paired mode, this doesn't apply since they don't have cCRE
    }
    return items;
  }

  function startDownload() {
    const filtered = getFilteredItems();
    const toDownload = selectedItems.size > 0
      ? filtered.filter((_: any, i: number) => selectedItems.has(i))
      : filtered;

    const finalList = limit ? toDownload.slice(0, limit) : toDownload;

    setPhase('downloading');

    if (paired) {
      downloader.call('download_paired', {
        cell_line: cellLine,
        one_per_cell: onePerCell,
        limit: finalList.length,
        tissues,
        min_size_gb: minSizeGb,
        max_size_gb: maxSizeGb,
      }).then(() => setPhase('done'));
    } else {
      downloader.call('download_mcool_only', {
        cell_line: cellLine,
        limit: finalList.length,
      }).then(() => setPhase('done'));
    }
  }

  // --- Scanning Phase ---
  if (phase === 'scanning' || finder.loading) {
    return (
      <Box flexDirection="column">
        <BoxPanel title={`${icons.search} Scanning Available Data`} titleColor={theme.cyan}>
          <Spinner label={paired ? "Querying 4DN & ENCODE for paired datasets (mcool + cCRE)..." : "Querying 4DN for .mcool files..."} />
          <Text color={theme.muted}>This may take a moment. Press q/Esc to cancel.</Text>
        </BoxPanel>
      </Box>
    );
  }

  if (finder.error) {
    return (
      <Box flexDirection="column">
        <BoxPanel title={`${icons.warning} Error`} titleColor={theme.error}>
          <Text color={theme.error}>{finder.error}</Text>
          <Text color={theme.muted}>Press q to go back</Text>
        </BoxPanel>
      </Box>
    );
  }

  const allItems = getItems();
  const items = getFilteredItems();

  // --- Preview Phase ---
  if (phase === 'preview') {
    const totalSize = paired
      ? items.reduce((s: number, p: any) => s + (p.total_size_gb || 0), 0)
      : items.reduce((s: number, f: any) => s + ((f.file_size || 0) / (1024 ** 3)), 0);

    const selectedSize = selectedItems.size > 0
      ? items.filter((_: any, i: number) => selectedItems.has(i))
          .reduce((s: number, p: any) => s + (p.total_size_gb || p.file_size / (1024 ** 3) || 0), 0)
      : totalSize;

    const totalPages = Math.max(1, Math.ceil(items.length / pageSize));
    const currentPage = Math.min(page, totalPages - 1);
    const pageItems = items.slice(currentPage * pageSize, (currentPage + 1) * pageSize);

    return (
      <Box flexDirection="column">
        {/* Filter bar */}
        <Box marginBottom={0}>
          {(['all', 'human', 'mouse'] as SpeciesFilter[]).map(f => (
            <Box key={f} marginRight={1}>
              <Text color={speciesFilter === f ? theme.cyan : theme.muted} bold={speciesFilter === f} underline={speciesFilter === f}>
                {f === 'all' ? `All (${allItems.length})` : f === 'human' ? 'Human' : 'Mouse'}
              </Text>
            </Box>
          ))}
          <Text color={theme.muted}> {icons.dot} </Text>
          <Text color={theme.muted}>Showing {items.length} of {allItems.length}</Text>
        </Box>

        <BoxPanel title={`${icons.download} Download Manager — ${items.length} datasets (${gbFormat(totalSize)})`} titleColor={theme.cyan}>
          {selectedItems.size > 0 && (
            <Box marginBottom={0}>
              <Text color={theme.green} bold>Selected: {selectedItems.size}</Text>
              <Text color={theme.muted}> ({gbFormat(selectedSize)})</Text>
            </Box>
          )}

          {/* Table Header */}
          <Box>
            <Text color={theme.blue} bold>{padR('', 4)}</Text>
            <Text color={theme.blue} bold>{padR('Cell Line', 18)}</Text>
            {paired && <Text color={theme.blue} bold>{padR('Tissue', 18)}</Text>}
            <Text color={theme.blue} bold>{padR('mcool', 14)}</Text>
            <Text color={theme.blue} bold>{padR('Size', 10)}</Text>
            {paired && <Text color={theme.blue} bold>{padR('cCRE', 14)}</Text>}
            {paired && <Text color={theme.blue} bold>{padR('Asm', 8)}</Text>}
            <Text color={theme.blue} bold>Tx</Text>
          </Box>
          <Text color={theme.border}>{'─'.repeat(paired ? 92 : 50)}</Text>

          {pageItems.map((item: any, i: number) => {
            const absIdx = currentPage * pageSize + i;
            const isSelected = selectedItems.has(absIdx);
            const isCursor = i === selectedIdx;
            const prefix = isCursor ? `${icons.arrow} ` : '  ';
            const check = isSelected ? `${icons.check} ` : `${icons.circle} `;
            const hasTreatment = !!(item.treatment || item.condition);
            const txLabel = hasTreatment ? 'Tx' : '--';
            const txColor = hasTreatment ? theme.warning : theme.muted;

            if (paired) {
              return (
                <Box key={i}>
                  <Text color={isCursor ? theme.accent : theme.muted}>{prefix}</Text>
                  <Text color={isSelected ? theme.green : theme.muted}>{check}</Text>
                  <Text color={isCursor ? theme.white : theme.fg}>{padR(item.cell_line || '', 18)}</Text>
                  <Text color={theme.muted}>{padR(item.tissue || '', 18)}</Text>
                  <Text color={theme.cyan}>{padR(item.mcool_accession || '', 14)}</Text>
                  <Text color={theme.orange}>{padR(gbFormat(item.mcool_size_gb || 0), 10)}</Text>
                  <Text color={theme.purple}>{padR(item.ccre_accession || '', 14)}</Text>
                  <Text color={theme.muted}>{padR(item.ccre_assembly || '?', 8)}</Text>
                  <Text color={txColor}>{txLabel}</Text>
                </Box>
              );
            } else {
              return (
                <Box key={i}>
                  <Text color={isCursor ? theme.accent : theme.muted}>{prefix}</Text>
                  <Text color={isSelected ? theme.green : theme.muted}>{check}</Text>
                  <Text color={isCursor ? theme.white : theme.fg}>{padR(item.cell_line || '', 18)}</Text>
                  <Text color={theme.cyan}>{padR(item.accession || '', 14)}</Text>
                  <Text color={theme.orange}>{padR(gbFormat((item.file_size || 0) / (1024 ** 3)), 10)}</Text>
                  <Text color={txColor}>{txLabel}</Text>
                </Box>
              );
            }
          })}

          {items.length > pageSize && (
            <Text color={theme.muted}>  Page {currentPage + 1}/{totalPages} ({items.length} total)</Text>
          )}

          <Text color={theme.border}>{'─'.repeat(paired ? 92 : 50)}</Text>
          <Box>
            <Text color={theme.muted}>[</Text><Text color={theme.cyan}>↑↓</Text>
            <Text color={theme.muted}>] Nav  [</Text><Text color={theme.cyan}>←→</Text>
            <Text color={theme.muted}>] Page  [</Text><Text color={theme.cyan}>Tab</Text>
            <Text color={theme.muted}>] Species  [</Text><Text color={theme.cyan}>Space</Text>
            <Text color={theme.muted}>] Toggle  [</Text><Text color={theme.cyan}>a</Text>
            <Text color={theme.muted}>] All  [</Text><Text color={theme.green}>d</Text>
            <Text color={theme.muted}>] DL  [</Text><Text color={theme.cyan}>q</Text>
            <Text color={theme.muted}>] Back</Text>
          </Box>
        </BoxPanel>
      </Box>
    );
  }

  // --- Downloading Phase ---
  if (phase === 'downloading') {
    const downloadCount = selectedItems.size > 0 ? selectedItems.size : items.length;
    const downloadSize = selectedItems.size > 0
      ? items.filter((_: any, i: number) => selectedItems.has(i))
          .reduce((s: number, p: any) => s + (p.total_size_gb || p.mcool_size_gb || (p.file_size || 0) / (1024 ** 3) || 0), 0)
      : items.reduce((s: number, p: any) => s + (p.total_size_gb || p.mcool_size_gb || (p.file_size || 0) / (1024 ** 3) || 0), 0);

    return (
      <Box flexDirection="column">
        <BoxPanel title={`${icons.download} Downloading ${downloadCount} items (${gbFormat(downloadSize)})`} titleColor={theme.cyan}>
          <Spinner label="Downloading files with parallel threads..." />
          <Box flexDirection="column" marginTop={1}>
            <Text color={theme.muted}>Items: {downloadCount} {paired ? 'paired datasets (mcool + cCRE)' : 'mcool files'}</Text>
            <Text color={theme.muted}>Total size: ~{gbFormat(downloadSize)}</Text>
            <Text color={theme.muted}>Progress updates stream to stderr.</Text>
          </Box>
          <Box marginTop={1}>
            <Text color={theme.muted}>Press </Text>
            <Text color={theme.cyan}>q</Text>
            <Text color={theme.muted}> to cancel and go back</Text>
          </Box>
        </BoxPanel>
      </Box>
    );
  }

  // --- Done Phase ---
  if (phase === 'done') {
    const results = downloader.data?.results || [];
    const successCount = results.filter((r: any) => r.success).length;
    const failCount = results.filter((r: any) => !r.success).length;

    return (
      <Box flexDirection="column">
        <BoxPanel title={`${icons.check} Download Complete`} titleColor={theme.success}>
          <Box flexDirection="column">
            <Box>
              <Text color={theme.success} bold>{icons.check} {successCount} succeeded</Text>
              {failCount > 0 && (
                <Text color={theme.error}> {icons.dot} {icons.cross} {failCount} failed</Text>
              )}
            </Box>

            {results.slice(0, 15).map((r: any, i: number) => (
              <Box key={i}>
                <Text color={r.success ? theme.success : theme.error}>
                  {r.success ? icons.check : icons.cross}
                </Text>
                <Text color={theme.muted}> {r.type} </Text>
                <Text color={theme.white}>{r.cell_line}</Text>
                <Text color={theme.muted}> ({r.accession})</Text>
              </Box>
            ))}

            {results.length > 15 && (
              <Text color={theme.muted}>  ... and {results.length - 15} more</Text>
            )}
          </Box>
          <Box marginTop={1}>
            <Text color={theme.muted}>Press </Text>
            <Text color={theme.cyan}>q</Text>
            <Text color={theme.muted}> to go back</Text>
          </Box>
        </BoxPanel>
      </Box>
    );
  }

  return null;
}

function padR(s: string, n: number): string {
  return s.length >= n ? s.slice(0, n) : s + ' '.repeat(n - s.length);
}
