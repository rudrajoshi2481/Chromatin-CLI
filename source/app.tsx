import React, { useState } from 'react';
import { Box, Text, useInput, useApp } from 'ink';
import { theme } from './theme/colors.js';
import Header from './components/Header.js';
import MainMenu from './components/MainMenu.js';
import Dashboard from './components/Dashboard.js';
import CellLineBrowser from './components/CellLineBrowser.js';
import DownloadJobs from './components/DownloadJobs.js';
import Statistics from './components/Statistics.js';
import QueryRunner from './components/QueryRunner.js';
import FetchData from './components/FetchData.js';
import Config from './components/Config.js';

export type View = 'menu' | 'dashboard' | 'browse' | 'download' | 'stats' | 'query' | 'fetch' | 'config';

export interface AppFlags {
  // Direct action modes (non-interactive)
  list?: boolean;
  fetch?: boolean;
  report?: boolean;
  testDownload?: boolean;
  checkDuplicates?: boolean;

  // Filters
  paired?: boolean;
  nonPaired?: boolean;
  cellLine?: string;
  tissues?: string[];
  onePerCell?: boolean;
  minReplicates?: number;
  maxReplicates?: number;
  minSizeGb?: number;
  maxSizeGb?: number;
  limit?: number;

  // Download
  download?: boolean;
  downloadAll?: boolean;

  // Options
  verbose?: boolean;
  noCache?: boolean;
  output?: string;
}

interface AppProps {
  flags: AppFlags;
}

export default function App({ flags }: AppProps) {
  const { exit } = useApp();
  const [view, setView] = useState<View>(flags.fetch ? 'fetch' : 'menu');
  const [menuIdx, setMenuIdx] = useState(0);
  const [isFetchCli, setIsFetchCli] = useState(!!flags.fetch);

  // Only handle quit from menu — other views handle their own back
  useInput((input, key) => {
    if (view === 'menu' && (input === 'q' || (key.ctrl && input === 'c'))) {
      exit();
    }
  });

  function handleNavigate(newView: string) {
    setView(newView as View);
  }

  function handleBack() {
    setView('menu');
  }

  // If launched with --fetch, exit after fetch completes
  function handleFetchDone() {
    if (isFetchCli) {
      exit();
    } else {
      setView('menu');
    }
  }

  return (
    <Box flexDirection="column" key={view}>
      <Header subtitle={view === 'fetch' ? 'Fetching data from APIs...' : undefined} />

      {view === 'menu' && (
        <MainMenu
          key="menu"
          onSelect={handleNavigate}
          selectedIdx={menuIdx}
          onChangeIdx={setMenuIdx}
        />
      )}

      {view === 'dashboard' && (
        <Dashboard key="dashboard" onNavigate={handleNavigate} />
      )}

      {view === 'browse' && (
        <CellLineBrowser
          key="browse"
          filter={flags.cellLine}
          pairedOnly={flags.paired}
          tissues={flags.tissues}
          onBack={handleBack}
        />
      )}

      {view === 'download' && (
        <DownloadJobs key="download" onBack={handleBack} />
      )}

      {view === 'stats' && (
        <Statistics key="stats" onBack={handleBack} />
      )}

      {view === 'query' && (
        <QueryRunner key="query" onBack={handleBack} />
      )}

      {view === 'fetch' && (
        <FetchData key="fetch" onDone={handleFetchDone} />
      )}

      {view === 'config' && (
        <Config key="config" onBack={handleBack} />
      )}
    </Box>
  );
}
