import React from 'react';
import { Box, Text, useInput } from 'ink';
import { theme, icons } from '../theme/colors.js';
import BoxPanel from './BoxPanel.js';

interface MainMenuProps {
  onSelect: (view: string) => void;
  selectedIdx: number;
  onChangeIdx: (idx: number) => void;
}

const MENU_ITEMS = [
  { key: 'dashboard', label: 'Dashboard', desc: 'Overview stats & summary', icon: icons.chart, color: theme.blue },
  { key: 'browse', label: 'Browse Cell Lines', desc: 'Interactive cell line browser', icon: icons.dna, color: theme.purple },
  { key: 'download', label: 'Download Jobs', desc: 'Monitor background downloads', icon: icons.download, color: theme.cyan },
  { key: 'stats', label: 'Statistics & Graphs', desc: 'Charts, distributions, sparklines', icon: icons.star, color: theme.orange },
  { key: 'query', label: 'SQL Query Runner', desc: 'Run custom DuckDB queries', icon: icons.database, color: theme.teal },
  { key: 'fetch', label: 'Refresh Data', desc: 'Re-fetch from 4DN/ENCODE APIs', icon: icons.search, color: theme.green },
  { key: 'config', label: 'Settings', desc: 'API keys, paths, export metadata', icon: icons.config, color: theme.muted },
];

export default function MainMenu({ onSelect, selectedIdx, onChangeIdx }: MainMenuProps) {
  useInput((input, key) => {
    if (key.upArrow) {
      onChangeIdx(Math.max(0, selectedIdx - 1));
    }
    if (key.downArrow) {
      onChangeIdx(Math.min(MENU_ITEMS.length - 1, selectedIdx + 1));
    }
    if (key.return) {
      onSelect(MENU_ITEMS[selectedIdx]!.key);
    }
    // Number shortcuts
    const num = parseInt(input, 10);
    if (num >= 1 && num <= MENU_ITEMS.length) {
      onSelect(MENU_ITEMS[num - 1]!.key);
    }
  });

  return (
    <BoxPanel title={`${icons.arrow} Quick Actions`} titleColor={theme.accent}>
      {MENU_ITEMS.map((item, i) => {
        const isSelected = i === selectedIdx;
        return (
          <Box key={item.key}>
            <Text color={isSelected ? theme.accent : theme.muted}>
              {isSelected ? `${icons.arrow} ` : '  '}
            </Text>
            <Text color={theme.muted}>{i + 1}. </Text>
            <Text color={isSelected ? item.color : theme.fg} bold={isSelected}>
              {item.icon} {item.label}
            </Text>
            <Text color={theme.muted}> — {item.desc}</Text>
          </Box>
        );
      })}
      <Box marginTop={1}>
        <Text color={theme.muted}>
          [<Text color={theme.cyan}>↑↓</Text>] Navigate  [<Text color={theme.cyan}>Enter</Text>] Select  [<Text color={theme.cyan}>1-7</Text>] Quick select  [<Text color={theme.cyan}>q</Text>] Quit
        </Text>
      </Box>
    </BoxPanel>
  );
}
