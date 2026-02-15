import React, { useEffect, useState } from 'react';
import { Box, Text, useInput } from 'ink';
import { theme, icons } from '../theme/colors.js';
import { usePython } from '../hooks/use-python.js';
import { useTerminalSize } from '../hooks/use-terminal-size.js';
import BoxPanel from './BoxPanel.js';
import Spinner from './Spinner.js';
import { gbFormat } from '../lib/format.js';

interface CellLineBrowserProps {
  filter?: string;
  pairedOnly?: boolean;
  tissues?: string[];
  onBack: () => void;
}

type SpeciesTab = 'all' | 'human' | 'mouse' | 'other';
type SortField = 'replicates' | 'size' | 'name' | 'ccre';
type SubView = 'list' | 'detail' | 'download_confirm';

const SPECIES_TABS: { key: SpeciesTab; label: string }[] = [
  { key: 'all', label: 'All' },
  { key: 'human', label: 'Human' },
  { key: 'mouse', label: 'Mouse' },
  { key: 'other', label: 'Other' },
];

export default function CellLineBrowser({ filter, pairedOnly, tissues, onBack }: CellLineBrowserProps) {
  const { data, loading, error, call } = usePython();
  const detail = usePython();
  const dlJob = usePython();
  const [selectedIdx, setSelectedIdx] = useState(0);
  const [searchText] = useState(filter || '');
  const [sortBy, setSortBy] = useState<SortField>('replicates');
  const [page, setPage] = useState(0);
  const [speciesTab, setSpeciesTab] = useState<SpeciesTab>('all');
  const [ccreFilter, setCcreFilter] = useState(false);
  const [checkedCells, setCheckedCells] = useState<Set<string>>(new Set());
  const [subView, setSubView] = useState<SubView>('list');
  // Detail view state
  const [detailRepIdx, setDetailRepIdx] = useState(0);
  const [detailRepPage, setDetailRepPage] = useState(0);
  const [checkedReps, setCheckedReps] = useState<Set<string>>(new Set());
  // Download confirm
  const [dlStarted, setDlStarted] = useState(false);

  const { columns: termCols, rows: termRows } = useTerminalSize();
  const pageSize = Math.max(8, termRows - 14);
  const detailPageSize = Math.max(6, termRows - 20);
  const sepWidth = Math.min(200, termCols - 6);

  useEffect(() => {
    call('list_cell_lines');
  }, []);

  // --- Input handling ---
  useInput((input, key) => {
    // --- DETAIL VIEW ---
    if (subView === 'detail') {
      if (key.escape || input === 'q') {
        setSubView('list');
        return;
      }
      const reps: any[] = detail.data?.replicates || [];
      const totalDetailPages = Math.max(1, Math.ceil(reps.length / detailPageSize));
      if (key.upArrow) setDetailRepIdx(prev => Math.max(0, prev - 1));
      if (key.downArrow) setDetailRepIdx(prev => Math.min(Math.min(detailPageSize, reps.length) - 1, prev + 1));
      if (key.rightArrow) { setDetailRepPage(prev => Math.min(totalDetailPages - 1, prev + 1)); setDetailRepIdx(0); }
      if (key.leftArrow) { setDetailRepPage(prev => Math.max(0, prev - 1)); setDetailRepIdx(0); }
      if (input === ' ') {
        const absIdx = detailRepPage * detailPageSize + detailRepIdx;
        const rep = reps[absIdx];
        if (rep) {
          const acc = rep.accession;
          setCheckedReps(prev => {
            const next = new Set(prev);
            if (next.has(acc)) next.delete(acc); else next.add(acc);
            return next;
          });
        }
      }
      if (input === 'a') {
        setCheckedReps(prev => {
          if (prev.size === reps.length) return new Set();
          return new Set(reps.map((r: any) => r.accession));
        });
      }
      if (input === 'd') {
        // Download selected reps (or all if none selected)
        const cellLine = detail.data?.cell_line?.cell_line || '';
        const selectedAccessions = checkedReps.size > 0
          ? Array.from(checkedReps)
          : reps.map((r: any) => r.accession);
        const items = [{ cell_line: cellLine, accessions: selectedAccessions, include_ccre: true }];
        setDlStarted(true);
        dlJob.call('start_download_job', { items });
        setSubView('download_confirm');
      }
      return;
    }

    // --- DOWNLOAD CONFIRM ---
    if (subView === 'download_confirm') {
      if (key.escape || input === 'q' || key.return) {
        setSubView('list');
        setDlStarted(false);
        return;
      }
      return;
    }

    // --- LIST VIEW ---
    if (key.escape || input === 'q') {
      onBack();
      return;
    }
    if (key.upArrow) setSelectedIdx(prev => Math.max(0, prev - 1));
    if (key.downArrow) setSelectedIdx(prev => prev + 1);
    if (input === 's') {
      setSortBy(prev =>
        prev === 'replicates' ? 'ccre' : prev === 'ccre' ? 'size' : prev === 'size' ? 'name' : 'replicates'
      );
      setPage(0);
    }
    if (key.rightArrow || input === 'n') { setPage(prev => prev + 1); setSelectedIdx(0); }
    if (key.leftArrow || input === 'p') { setPage(prev => Math.max(0, prev - 1)); setSelectedIdx(0); }
    if (key.tab) {
      const tabs: SpeciesTab[] = ['all', 'human', 'mouse', 'other'];
      setSpeciesTab(prev => tabs[(tabs.indexOf(prev) + 1) % tabs.length]!);
      setPage(0); setSelectedIdx(0);
    }
    if (input === 'c') { setCcreFilter(prev => !prev); setPage(0); setSelectedIdx(0); }

    // Multi-select with Space
    if (input === ' ') {
      const cl = getCurrentCellLine();
      if (cl) {
        setCheckedCells(prev => {
          const next = new Set(prev);
          if (next.has(cl.cell_line)) next.delete(cl.cell_line); else next.add(cl.cell_line);
          return next;
        });
      }
    }

    // Select all visible
    if (input === 'a') {
      const filtered = getFilteredCellLines();
      setCheckedCells(prev => {
        if (prev.size === filtered.length) return new Set();
        return new Set(filtered.map((c: any) => c.cell_line));
      });
    }

    // Enter = expand detail
    if (key.return) {
      const cl = getCurrentCellLine();
      if (cl) {
        detail.call('get_cell_line_details', { cell_line: cl.cell_line });
        setDetailRepIdx(0);
        setDetailRepPage(0);
        setCheckedReps(new Set());
        setSubView('detail');
      }
    }

    // d = download selected cell lines
    if (input === 'd') {
      const selected = checkedCells.size > 0
        ? Array.from(checkedCells)
        : (() => { const cl = getCurrentCellLine(); return cl ? [cl.cell_line] : []; })();
      if (selected.length > 0) {
        const items = selected.map(cl => ({ cell_line: cl, include_ccre: true }));
        setDlStarted(true);
        dlJob.call('start_download_job', { items });
        setSubView('download_confirm');
      }
    }
  });

  function getFilteredCellLines(): any[] {
    let cls: any[] = data?.cell_lines || [];
    if (speciesTab === 'human') cls = cls.filter((c: any) => (c.species || '') === 'Homo sapiens');
    else if (speciesTab === 'mouse') cls = cls.filter((c: any) => (c.species || '') === 'Mus musculus');
    else if (speciesTab === 'other') cls = cls.filter((c: any) =>
      (c.species || '') !== 'Homo sapiens' && (c.species || '') !== 'Mus musculus');
    if (ccreFilter) cls = cls.filter((c: any) => c.has_ccre);
    if (searchText) {
      const q = searchText.toLowerCase();
      cls = cls.filter((c: any) => (c.cell_line || '').toLowerCase().includes(q) || (c.tissue || '').toLowerCase().includes(q));
    }
    if (pairedOnly) cls = cls.filter((c: any) => c.has_ccre && c.has_mcool);
    if (tissues && tissues.length > 0) {
      const tL = tissues.map(t => t.toLowerCase());
      cls = cls.filter((c: any) => tL.some(t => (c.tissue || '').toLowerCase().includes(t)));
    }
    cls = [...cls].sort((a, b) => {
      if (sortBy === 'replicates') return (b.replicates || 0) - (a.replicates || 0);
      if (sortBy === 'size') return (b.total_gb || 0) - (a.total_gb || 0);
      if (sortBy === 'ccre') {
        if (a.has_ccre !== b.has_ccre) return a.has_ccre ? -1 : 1;
        return (b.replicates || 0) - (a.replicates || 0);
      }
      return (a.cell_line || '').localeCompare(b.cell_line || '');
    });
    return cls;
  }

  function getCurrentCellLine(): any | null {
    const cls = getFilteredCellLines();
    const totalPages = Math.max(1, Math.ceil(cls.length / pageSize));
    const cp = Math.min(page, totalPages - 1);
    const items = cls.slice(cp * pageSize, (cp + 1) * pageSize);
    const idx = Math.min(selectedIdx, Math.max(0, items.length - 1));
    return items[idx] || null;
  }

  // --- Loading ---
  if (loading) return <Spinner label="Fetching cell lines from database..." />;
  if (error) return (
    <Box flexDirection="column">
      <Text color={theme.error}>{icons.cross} {error}</Text>
      <Text color={theme.muted}>Press q to go back</Text>
    </Box>
  );

  // === DOWNLOAD CONFIRM ===
  if (subView === 'download_confirm') {
    return (
      <Box flexDirection="column">
        <BoxPanel title={`${icons.download} Download Job Started`} titleColor={theme.success}>
          {dlJob.loading ? (
            <Spinner label="Starting background download..." />
          ) : dlJob.error ? (
            <Text color={theme.error}>{icons.cross} {dlJob.error}</Text>
          ) : (
            <Box flexDirection="column">
              <Text color={theme.success} bold>{icons.check} Background download job started!</Text>
              <Text color={theme.muted}>Job ID: <Text color={theme.cyan}>{dlJob.data?.job_id || '?'}</Text></Text>
              <Text color={theme.muted}>PID: <Text color={theme.white}>{dlJob.data?.pid || '?'}</Text></Text>
              <Text color={theme.muted}>The download will continue even if you close the app.</Text>
              <Text color={theme.muted}>Check progress in <Text color={theme.cyan}>Download Jobs</Text> (menu option 3).</Text>
            </Box>
          )}
          <Box marginTop={1}>
            <Text color={theme.muted}>Press <Text color={theme.cyan}>Enter</Text> or <Text color={theme.cyan}>q</Text> to go back</Text>
          </Box>
        </BoxPanel>
      </Box>
    );
  }

  // === DETAIL VIEW ===
  if (subView === 'detail') {
    if (detail.loading) return <Spinner label="Loading cell line details..." />;
    if (detail.error) return (
      <Box flexDirection="column">
        <Text color={theme.error}>{icons.cross} {detail.error}</Text>
        <Text color={theme.muted}>Press q to go back to list</Text>
      </Box>
    );

    const cl = detail.data?.cell_line || {};
    const reps: any[] = detail.data?.replicates || [];
    const ccreList: any[] = detail.data?.ccre || [];
    const txSummary: any[] = detail.data?.treatment_summary || [];
    const summary = detail.data?.summary || {};
    const totalDetailPages = Math.max(1, Math.ceil(reps.length / detailPageSize));
    const dPage = Math.min(detailRepPage, totalDetailPages - 1);
    const dItems = reps.slice(dPage * detailPageSize, (dPage + 1) * detailPageSize);
    const dIdx = Math.min(detailRepIdx, Math.max(0, dItems.length - 1));

    const speciesShort = (cl.species || '').includes('sapiens') ? 'Human'
      : (cl.species || '').includes('musculus') ? 'Mouse' : (cl.species || '').slice(0, 10);

    return (
      <Box flexDirection="column">
        {/* Cell line header */}
        <BoxPanel title={`${icons.dna} ${cl.cell_line || '?'}`} titleColor={theme.cyan}>
          <Box>
            <Box flexDirection="column" marginRight={3}>
              <Box><Text color={theme.muted}>Species:  </Text><Text color={theme.cyan}>{speciesShort}</Text></Box>
              <Box><Text color={theme.muted}>Tissue:   </Text><Text color={theme.white}>{cl.tissue || 'Unknown'}</Text></Box>
              <Box><Text color={theme.muted}>Assembly: </Text><Text color={theme.white}>{cl.genome_assembly || '?'}</Text></Box>
              <Box><Text color={theme.muted}>Type:     </Text><Text color={theme.white}>{cl.biosample_type || '?'}</Text></Box>
            </Box>
            <Box flexDirection="column" marginRight={3}>
              <Box><Text color={theme.muted}>Replicates: </Text><Text color={theme.orange} bold>{summary.total_replicates || 0}</Text></Box>
              <Box><Text color={theme.muted}>Total size: </Text><Text color={theme.cyan} bold>{gbFormat(summary.total_gb || 0)}</Text></Box>
              <Box><Text color={theme.muted}>Downloaded: </Text><Text color={theme.success}>{summary.downloaded || 0}</Text></Box>
              <Box><Text color={theme.muted}>Treatments: </Text><Text color={theme.warning}>{summary.unique_treatments || 0}</Text></Box>
            </Box>
            <Box flexDirection="column">
              <Box><Text color={theme.muted}>cCRE files: </Text><Text color={ccreList.length > 0 ? theme.success : theme.error}>{ccreList.length}</Text></Box>
              {ccreList.map((c: any, i: number) => {
                const mismatch = cl.genome_assembly && c.assembly && cl.genome_assembly !== c.assembly;
                return (
                  <Box key={i} flexDirection="column">
                    <Box><Text color={theme.purple}> {c.accession}</Text><Text color={mismatch ? theme.error : theme.muted}> ({c.assembly}) {(c.file_size_mb || 0).toFixed(1)} MB</Text></Box>
                    {mismatch && <Text color={theme.error}> {icons.warning} Assembly mismatch: chromatin={cl.genome_assembly} cCRE={c.assembly}</Text>}
                  </Box>
                );
              })}
              {ccreList.length === 0 && <Text color={theme.error}> No cCRE available</Text>}
            </Box>
          </Box>
        </BoxPanel>

        {/* Treatment summary */}
        {txSummary.length > 1 && (
          <Box marginBottom={0}>
            <Text color={theme.muted}>Treatments: </Text>
            {txSummary.slice(0, 4).map((t: any, i: number) => (
              <Box key={i} marginRight={1}>
                <Text color={t.treatment === 'untreated' ? theme.green : theme.warning}>
                  {t.treatment === 'untreated' ? 'untreated' : t.treatment.slice(0, 25)}
                </Text>
                <Text color={theme.muted}> ({t.count})</Text>
              </Box>
            ))}
          </Box>
        )}

        {/* Replicates table */}
        <BoxPanel title={`Replicates — Page ${dPage + 1}/${totalDetailPages} — ${checkedReps.size > 0 ? checkedReps.size + ' selected' : 'none selected'}`} titleColor={theme.orange}>
          <Box>
            <Text color={theme.blue} bold>{padR('', 5)}</Text>
            <Text color={theme.blue} bold>{padR('Accession', 16)}</Text>
            <Text color={theme.blue} bold>{padR('Size', 10)}</Text>
            <Text color={theme.blue} bold>{padR('Status', 12)}</Text>
            <Text color={theme.blue} bold>{padR('Treatment', 30)}</Text>
            <Text color={theme.blue} bold>Mod</Text>
          </Box>
          <Text color={theme.border}>{'─'.repeat(Math.min(85, sepWidth))}</Text>
          {dItems.map((rep: any, i: number) => {
            const isCursor = i === dIdx;
            const isChecked = checkedReps.has(rep.accession);
            const dlStatus = rep.download_status || 'pending';
            const dlColor = dlStatus === 'downloaded' ? theme.success : dlStatus === 'downloading' ? theme.cyan : theme.muted;
            const tx = rep.treatment || 'untreated';
            const mod = rep.modification || '';
            return (
              <Box key={i}>
                <Text color={isCursor ? theme.accent : theme.muted}>{isCursor ? icons.arrow : ' '} </Text>
                <Text color={isChecked ? theme.green : theme.muted}>{isChecked ? icons.check : icons.circle} </Text>
                <Text color={isCursor ? theme.white : theme.cyan}>{padR(rep.accession || '', 16)}</Text>
                <Text color={theme.orange}>{padR(gbFormat(rep.file_size_gb || 0), 10)}</Text>
                <Text color={dlColor}>{padR(dlStatus, 12)}</Text>
                <Text color={tx === 'untreated' ? theme.green : theme.warning}>{padR(tx.slice(0, 28), 30)}</Text>
                <Text color={theme.muted}>{mod.slice(0, 15)}</Text>
              </Box>
            );
          })}
          <Text color={theme.border}>{'─'.repeat(Math.min(85, sepWidth))}</Text>
          <Box>
            <Text color={theme.muted}>[</Text><Text color={theme.cyan}>↑↓</Text>
            <Text color={theme.muted}>] Nav  [</Text><Text color={theme.cyan}>←→</Text>
            <Text color={theme.muted}>] Page  [</Text><Text color={theme.cyan}>Space</Text>
            <Text color={theme.muted}>] Select  [</Text><Text color={theme.cyan}>a</Text>
            <Text color={theme.muted}>] All  [</Text><Text color={theme.green}>d</Text>
            <Text color={theme.muted}>] Download  [</Text><Text color={theme.cyan}>q</Text>
            <Text color={theme.muted}>] Back</Text>
          </Box>
        </BoxPanel>
      </Box>
    );
  }

  // === LIST VIEW ===
  const cellLines = getFilteredCellLines();
  const totalAll = data?.cell_lines?.length || 0;
  const totalHuman = (data?.cell_lines || []).filter((c: any) => c.species === 'Homo sapiens').length;
  const totalMouse = (data?.cell_lines || []).filter((c: any) => c.species === 'Mus musculus').length;
  const totalOther = totalAll - totalHuman - totalMouse;
  const withCcre = cellLines.filter((c: any) => c.has_ccre).length;
  const totalPages = Math.max(1, Math.ceil(cellLines.length / pageSize));
  const currentPage = Math.min(page, totalPages - 1);
  const pageItems = cellLines.slice(currentPage * pageSize, (currentPage + 1) * pageSize);
  const clampedIdx = Math.min(selectedIdx, Math.max(0, pageItems.length - 1));

  const selectedGb = Array.from(checkedCells).reduce((sum, name) => {
    const cl = (data?.cell_lines || []).find((c: any) => c.cell_line === name);
    return sum + (cl?.total_gb || 0);
  }, 0);

  return (
    <Box flexDirection="column">
      {/* Species Tabs + Selection info */}
      <Box marginBottom={0}>
        {SPECIES_TABS.map((tab) => {
          const isActive = speciesTab === tab.key;
          const count = tab.key === 'all' ? totalAll
            : tab.key === 'human' ? totalHuman
            : tab.key === 'mouse' ? totalMouse : totalOther;
          return (
            <Box key={tab.key} marginRight={1}>
              <Text color={isActive ? theme.cyan : theme.muted} bold={isActive} underline={isActive}>
                {tab.label} ({count})
              </Text>
            </Box>
          );
        })}
        <Text color={theme.muted}> {icons.dot} </Text>
        <Text color={ccreFilter ? theme.green : theme.muted} bold={ccreFilter}>
          cCRE: {ccreFilter ? 'ON' : 'off'}
        </Text>
        <Text color={theme.muted}> ({withCcre})</Text>
        {checkedCells.size > 0 && (
          <>
            <Text color={theme.muted}> {icons.dot} </Text>
            <Text color={theme.green} bold>Selected: {checkedCells.size}</Text>
            <Text color={theme.muted}> ({gbFormat(selectedGb)})</Text>
          </>
        )}
      </Box>

      <BoxPanel
        title={`${icons.dna} Cell Line Browser — ${cellLines.length} results`}
        titleColor={theme.blue}
      >
        <Box marginBottom={0}>
          <Text color={theme.muted}>Sort: </Text><Text color={theme.cyan}>{sortBy}</Text>
          <Text color={theme.muted}> {icons.dot} Page {currentPage + 1}/{totalPages}</Text>
          {searchText ? <Text color={theme.muted}> {icons.dot} Filter: "{searchText}"</Text> : null}
        </Box>

        {/* Header */}
        <Box>
          <Text color={theme.blue} bold>{padR('', 5)}</Text>
          <Text color={theme.blue} bold>{padR('Cell Line', 80)}</Text>
          <Text color={theme.blue} bold>{padR('Species', 8)}</Text>
          <Text color={theme.blue} bold>{padR('Tissue', 50)}</Text>
          <Text color={theme.blue} bold>{padR('Reps', 5)}</Text>
          <Text color={theme.blue} bold>{padR('Size', 10)}</Text>
          <Text color={theme.blue} bold>{padR('cCRE', 5)}</Text>
          <Text color={theme.blue} bold>{padR('Asm', 15)}</Text>
          <Text color={theme.blue} bold>{padR('Tx', 4)}</Text>
          <Text color={theme.blue} bold>ML</Text>
        </Box>
        <Text color={theme.border}>{'─'.repeat(sepWidth)}</Text>

        {pageItems.length === 0 ? (
          <Text color={theme.muted}>No cell lines match the current filters.</Text>
        ) : (
          pageItems.map((cl: any, i: number) => {
            const isCursor = i === clampedIdx;
            const isChecked = checkedCells.has(cl.cell_line);
            const hasPair = cl.has_ccre && cl.has_mcool;
            const sp = (cl.species || '').includes('sapiens') ? 'Hum'
              : (cl.species || '').includes('musculus') ? 'Mus'
              : (cl.species || '').includes('rerio') ? 'Zeb' : '?';
            const spColor = sp === 'Hum' ? theme.cyan : sp === 'Mus' ? theme.orange : theme.purple;
            const mlStatus = hasPair ? 'Ready' : cl.has_mcool ? 'NoCRE' : 'None';
            const mlColor = hasPair ? theme.success : cl.has_mcool ? theme.warning : theme.error;
            const txCount = (cl.treated_count || 0);

            return (
              <Box key={i}>
                <Text color={isCursor ? theme.accent : theme.muted}>{isCursor ? icons.arrow : ' '} </Text>
                <Text color={isChecked ? theme.green : theme.muted}>{isChecked ? icons.check : icons.circle} </Text>
                <Text color={isCursor ? theme.white : theme.fg} bold={isCursor}>
                  {padR(cl.cell_line || '', 80)}
                </Text>
                <Text color={spColor}>{padR(sp, 8)}</Text>
                <Text color={theme.muted}>{padR(cl.tissue || 'Unknown', 50)}</Text>
                <Text color={theme.orange}>{padR(String(cl.replicates || 0), 5)}</Text>
                <Text color={theme.cyan}>{padR(gbFormat(cl.total_gb || 0), 10)}</Text>
                <Text color={cl.has_ccre ? theme.success : theme.error}>
                  {padR(cl.has_ccre ? icons.check : icons.cross, 5)}
                </Text>
                <Text color={theme.muted}>{padR(cl.genome_assembly || '?', 15)}</Text>
                <Text color={txCount > 0 ? theme.warning : theme.muted}>{padR(txCount > 0 ? String(txCount) : '--', 4)}</Text>
                <Text color={mlColor}>{mlStatus}</Text>
              </Box>
            );
          })
        )}

        <Text color={theme.border}>{'─'.repeat(sepWidth)}</Text>
        <Box>
          <Text color={theme.muted}>[</Text><Text color={theme.cyan}>↑↓</Text>
          <Text color={theme.muted}>] Nav  [</Text><Text color={theme.cyan}>←→</Text>
          <Text color={theme.muted}>] Page  [</Text><Text color={theme.cyan}>Enter</Text>
          <Text color={theme.muted}>] Details  [</Text><Text color={theme.cyan}>Space</Text>
          <Text color={theme.muted}>] Select  [</Text><Text color={theme.cyan}>a</Text>
          <Text color={theme.muted}>] All</Text>
        </Box>
        <Box>
          <Text color={theme.muted}>[</Text><Text color={theme.green}>d</Text>
          <Text color={theme.muted}>] Download  [</Text><Text color={theme.cyan}>Tab</Text>
          <Text color={theme.muted}>] Species  [</Text><Text color={theme.cyan}>c</Text>
          <Text color={theme.muted}>] cCRE  [</Text><Text color={theme.cyan}>s</Text>
          <Text color={theme.muted}>] Sort  [</Text><Text color={theme.cyan}>q</Text>
          <Text color={theme.muted}>] Back</Text>
        </Box>
      </BoxPanel>
    </Box>
  );
}

function padR(s: string, n: number): string {
  return s.length >= n ? s.slice(0, n) : s + ' '.repeat(n - s.length);
}
