import React, { useEffect, useState, useRef } from 'react';
import { Box, Text, useInput } from 'ink';
import { theme, icons } from '../theme/colors.js';
import { usePython } from '../hooks/use-python.js';
import { useTerminalSize } from '../hooks/use-terminal-size.js';
import BoxPanel from './BoxPanel.js';
import Spinner from './Spinner.js';
import { gbFormat } from '../lib/format.js';

interface DownloadJobsProps {
  onBack: () => void;
}

export default function DownloadJobs({ onBack }: DownloadJobsProps) {
  const jobsList = usePython();
  const jobAction = usePython();
  const [selectedIdx, setSelectedIdx] = useState(0);
  const [expandedJob, setExpandedJob] = useState<string | null>(null);
  const [expandedData, setExpandedData] = useState<any>(null);
  const [resultPage, setResultPage] = useState(0);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const { columns: termCols, rows: termRows } = useTerminalSize();
  const resultPageSize = Math.max(6, termRows - 18);
  const sepWidth = Math.min(88, termCols - 6);

  // Poll for job updates every 3 seconds
  useEffect(() => {
    jobsList.call('list_download_jobs');
    pollRef.current = setInterval(() => {
      if (expandedJob) {
        jobAction.call('get_job_status', { job_id: expandedJob }).then((data: any) => {
          if (data) setExpandedData(data);
        });
      } else {
        jobsList.call('list_download_jobs');
      }
    }, 3000);
    return () => { if (pollRef.current) clearInterval(pollRef.current); };
  }, [expandedJob]);

  useInput((input, key) => {
    // Expanded job view
    if (expandedJob) {
      if (key.escape || input === 'q') {
        setExpandedJob(null);
        setExpandedData(null);
        setResultPage(0);
        jobsList.call('list_download_jobs');
        return;
      }
      if (key.rightArrow) setResultPage(prev => prev + 1);
      if (key.leftArrow) setResultPage(prev => Math.max(0, prev - 1));

      // Stop job
      if (input === 'x') {
        const job = expandedData;
        if (job && (job.status === 'running' || job.status === 'starting')) {
          jobAction.call('stop_download_job', { job_id: expandedJob });
        }
      }
      return;
    }

    // List view
    if (key.escape || input === 'q') {
      onBack();
      return;
    }

    const jobs: any[] = jobsList.data?.jobs || [];
    if (key.upArrow) setSelectedIdx(prev => Math.max(0, prev - 1));
    if (key.downArrow) setSelectedIdx(prev => Math.min(jobs.length - 1, prev + 1));

    // Enter = expand job details
    if (key.return) {
      const job = jobs[selectedIdx];
      if (job) {
        setExpandedJob(job.job_id);
        jobAction.call('get_job_status', { job_id: job.job_id }).then((data: any) => {
          if (data) setExpandedData(data);
        });
        setResultPage(0);
      }
    }

    // x = stop selected job
    if (input === 'x') {
      const job = jobs[selectedIdx];
      if (job && (job.status === 'running' || job.status === 'starting')) {
        jobAction.call('stop_download_job', { job_id: job.job_id }).then(() => {
          jobsList.call('list_download_jobs');
        });
      }
    }

    // Delete completed/failed job
    if (key.delete || input === 'X') {
      const job = jobs[selectedIdx];
      if (job && job.status !== 'running' && job.status !== 'starting') {
        jobAction.call('delete_download_job', { job_id: job.job_id }).then(() => {
          jobsList.call('list_download_jobs');
        });
      }
    }

    // r = refresh
    if (input === 'r') {
      jobsList.call('list_download_jobs');
    }
  });

  // === EXPANDED JOB VIEW ===
  if (expandedJob) {
    const job = expandedData || {};
    const results: any[] = job.results || [];
    const totalResultPages = Math.max(1, Math.ceil(results.length / resultPageSize));
    const rPage = Math.min(resultPage, totalResultPages - 1);
    const rItems = results.slice(rPage * resultPageSize, (rPage + 1) * resultPageSize);
    const isRunning = job.status === 'running' || job.status === 'starting';
    const progress = job.total_tasks > 0
      ? Math.round(((job.completed || 0) + (job.failed || 0)) / job.total_tasks * 100)
      : 0;
    const barWidth = 30;
    const filled = Math.round(barWidth * progress / 100);
    const progressBar = '█'.repeat(filled) + '░'.repeat(barWidth - filled);

    return (
      <Box flexDirection="column">
        <BoxPanel title={`${icons.download} Job: ${job.job_id || expandedJob}`} titleColor={statusColor(job.status)}>
          <Box>
            <Box flexDirection="column" marginRight={3}>
              <Box><Text color={theme.muted}>Status:   </Text><Text color={statusColor(job.status)} bold>{job.status || '?'}</Text></Box>
              <Box><Text color={theme.muted}>PID:      </Text><Text color={theme.white}>{job.pid || '?'}</Text></Box>
              <Box><Text color={theme.muted}>Started:  </Text><Text color={theme.white}>{formatTime(job.started_at)}</Text></Box>
              {job.finished_at && <Box><Text color={theme.muted}>Finished: </Text><Text color={theme.white}>{formatTime(job.finished_at)}</Text></Box>}
            </Box>
            <Box flexDirection="column" marginRight={3}>
              <Box><Text color={theme.muted}>Tasks:     </Text><Text color={theme.orange}>{job.total_tasks || 0}</Text></Box>
              <Box><Text color={theme.muted}>Completed: </Text><Text color={theme.success}>{job.completed || 0}</Text></Box>
              <Box><Text color={theme.muted}>Failed:    </Text><Text color={theme.error}>{job.failed || 0}</Text></Box>
              <Box><Text color={theme.muted}>Total:     </Text><Text color={theme.cyan}>{gbFormat(job.total_gb || 0)}</Text></Box>
            </Box>
            <Box flexDirection="column">
              <Box><Text color={theme.muted}>Cell lines: </Text><Text color={theme.white}>{(job.cell_lines || []).join(', ').slice(0, 40)}</Text></Box>
              {isRunning && job.current && (
                <Box><Text color={theme.cyan}>{icons.circle} {job.current}</Text></Box>
              )}
            </Box>
          </Box>

          {/* Progress bar */}
          <Box marginTop={1}>
            <Text color={theme.muted}>Progress: </Text>
            <Text color={progress >= 100 ? theme.success : theme.cyan}>{progressBar}</Text>
            <Text color={theme.white} bold> {progress}%</Text>
          </Box>
        </BoxPanel>

        {/* Results table */}
        {results.length > 0 && (
          <BoxPanel title={`Results — Page ${rPage + 1}/${totalResultPages}`} titleColor={theme.teal}>
            <Box>
              <Text color={theme.blue} bold>{padR('Type', 6)}</Text>
              <Text color={theme.blue} bold>{padR('Cell Line', 22)}</Text>
              <Text color={theme.blue} bold>{padR('Accession', 16)}</Text>
              <Text color={theme.blue} bold>{padR('Size', 10)}</Text>
              <Text color={theme.blue} bold>Status</Text>
            </Box>
            <Text color={theme.border}>{'─'.repeat(Math.min(65, sepWidth))}</Text>
            {rItems.map((r: any, i: number) => (
              <Box key={i}>
                <Text color={r.type === 'mcool' ? theme.cyan : theme.purple}>{padR(r.type || '', 6)}</Text>
                <Text color={theme.white}>{padR(r.cell_line || '', 22)}</Text>
                <Text color={theme.muted}>{padR(r.accession || '', 16)}</Text>
                <Text color={theme.orange}>{padR(gbFormat(r.size_gb || 0), 10)}</Text>
                <Text color={r.success ? theme.success : theme.error}>
                  {r.success ? icons.check + ' done' : icons.cross + ' fail'}
                </Text>
              </Box>
            ))}
            <Text color={theme.border}>{'─'.repeat(Math.min(65, sepWidth))}</Text>
          </BoxPanel>
        )}

        {/* Log tail */}
        {job.log_tail && (
          <BoxPanel title="Log (last lines)" titleColor={theme.muted}>
            <Text color={theme.muted}>{job.log_tail.slice(-300)}</Text>
          </BoxPanel>
        )}

        <Box>
          <Text color={theme.muted}>[</Text><Text color={theme.cyan}>←→</Text>
          <Text color={theme.muted}>] Page  </Text>
          {isRunning && (
            <>
              <Text color={theme.muted}>[</Text><Text color={theme.error}>x</Text>
              <Text color={theme.muted}>] Stop  </Text>
            </>
          )}
          <Text color={theme.muted}>[</Text><Text color={theme.cyan}>q</Text>
          <Text color={theme.muted}>] Back  </Text>
          <Text color={theme.muted}>(auto-refreshes every 3s)</Text>
        </Box>
      </Box>
    );
  }

  // === JOBS LIST VIEW ===
  if (jobsList.loading && !jobsList.data) {
    return <Spinner label="Loading download jobs..." />;
  }

  const jobs: any[] = jobsList.data?.jobs || [];
  const runningCount = jobs.filter(j => j.status === 'running' || j.status === 'starting').length;
  const doneCount = jobs.filter(j => j.status === 'done').length;
  const idx = Math.min(selectedIdx, Math.max(0, jobs.length - 1));

  return (
    <Box flexDirection="column">
      <BoxPanel
        title={`${icons.download} Download Jobs — ${jobs.length} jobs (${runningCount} running, ${doneCount} done)`}
        titleColor={theme.cyan}
      >
        {jobs.length === 0 ? (
          <Box flexDirection="column">
            <Text color={theme.muted}>No download jobs yet.</Text>
            <Text color={theme.muted}>Go to <Text color={theme.cyan}>Browse Cell Lines</Text> and press <Text color={theme.green}>d</Text> to start a download.</Text>
          </Box>
        ) : (
          <>
            <Box>
              <Text color={theme.blue} bold>{padR('', 3)}</Text>
              <Text color={theme.blue} bold>{padR('Job ID', 10)}</Text>
              <Text color={theme.blue} bold>{padR('Status', 10)}</Text>
              <Text color={theme.blue} bold>{padR('Progress', 14)}</Text>
              <Text color={theme.blue} bold>{padR('Tasks', 8)}</Text>
              <Text color={theme.blue} bold>{padR('Size', 10)}</Text>
              <Text color={theme.blue} bold>{padR('Cell Lines', 30)}</Text>
            </Box>
            <Text color={theme.border}>{'─'.repeat(sepWidth)}</Text>
            {jobs.map((job: any, i: number) => {
              const isCursor = i === idx;
              const total = job.total_tasks || 0;
              const done = (job.completed || 0) + (job.failed || 0);
              const pct = total > 0 ? Math.round(done / total * 100) : 0;
              const miniBar = '█'.repeat(Math.round(8 * pct / 100)) + '░'.repeat(8 - Math.round(8 * pct / 100));
              const cells = (job.cell_lines || []).join(', ');

              return (
                <Box key={i}>
                  <Text color={isCursor ? theme.accent : theme.muted}>
                    {isCursor ? `${icons.arrow} ` : '  '}
                  </Text>
                  <Text color={isCursor ? theme.white : theme.cyan}>{padR(job.job_id || '', 10)}</Text>
                  <Text color={statusColor(job.status)} bold>{padR(job.status || '?', 10)}</Text>
                  <Text color={theme.cyan}>{miniBar} </Text>
                  <Text color={theme.white}>{padR(pct + '%', 5)}</Text>
                  <Text color={theme.orange}>{padR(`${done}/${total}`, 8)}</Text>
                  <Text color={theme.muted}>{padR(gbFormat(job.total_gb || 0), 10)}</Text>
                  <Text color={theme.muted}>{cells.slice(0, 28)}</Text>
                </Box>
              );
            })}
            <Text color={theme.border}>{'─'.repeat(sepWidth)}</Text>
          </>
        )}

        <Box>
          <Text color={theme.muted}>[</Text><Text color={theme.cyan}>↑↓</Text>
          <Text color={theme.muted}>] Nav  [</Text><Text color={theme.cyan}>Enter</Text>
          <Text color={theme.muted}>] Details  [</Text><Text color={theme.error}>x</Text>
          <Text color={theme.muted}>] Stop  [</Text><Text color={theme.cyan}>r</Text>
          <Text color={theme.muted}>] Refresh  [</Text><Text color={theme.cyan}>q</Text>
          <Text color={theme.muted}>] Back</Text>
        </Box>
      </BoxPanel>
    </Box>
  );
}

function statusColor(status: string): string {
  switch (status) {
    case 'running': case 'starting': return theme.cyan;
    case 'done': return theme.success;
    case 'cancelled': case 'stopping': case 'stopped': return theme.warning;
    case 'error': case 'crashed': return theme.error;
    default: return theme.muted;
  }
}

function formatTime(iso: string | undefined): string {
  if (!iso) return '?';
  try {
    const d = new Date(iso);
    return d.toLocaleTimeString() + ' ' + d.toLocaleDateString();
  } catch { return iso; }
}

function padR(s: string, n: number): string {
  return s.length >= n ? s.slice(0, n) : s + ' '.repeat(n - s.length);
}
