import React from 'react';
import { Box, Text } from 'ink';
import { theme, icons } from '../theme/colors.js';

const LOGO = [
  '  ██████╗██╗  ██╗██████╗  ██████╗ ███╗   ███╗ █████╗ ████████╗██╗███╗   ██╗',
  ' ██╔════╝██║  ██║██╔══██╗██╔═══██╗████╗ ████║██╔══██╗╚══██╔══╝██║████╗  ██║',
  ' ██║     ███████║██████╔╝██║   ██║██╔████╔██║███████║   ██║   ██║██╔██╗ ██║',
  ' ██║     ██╔══██║██╔══██╗██║   ██║██║╚██╔╝██║██╔══██║   ██║   ██║██║╚██╗██║',
  ' ╚██████╗██║  ██║██║  ██║╚██████╔╝██║ ╚═╝ ██║██║  ██║   ██║   ██║██║ ╚████║',
  '  ╚═════╝╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝ ╚═╝     ╚═╝╚═╝  ╚═╝   ╚═╝   ╚═╝╚═╝  ╚═══╝',
];

interface HeaderProps {
  subtitle?: string;
}

export default function Header({ subtitle }: HeaderProps) {
  return (
    <Box flexDirection="column" marginBottom={1}>
      <Box flexDirection="column">
        {LOGO.map((line, i) => (
          <Text key={i} color={theme.blue}>{line}</Text>
        ))}
      </Box>
      <Box marginTop={0} marginLeft={2}>
        <Text color={theme.purple} bold>Data Manager</Text>
        <Text color={theme.muted}> v2.0.0</Text>
        <Text color={theme.muted}> {icons.dot} </Text>
        <Text color={theme.teal}>4DN + ENCODE</Text>
        <Text color={theme.muted}> {icons.dot} </Text>
        <Text color={theme.orange}>DuckDB</Text>
      </Box>
      {subtitle && (
        <Box marginLeft={2}>
          <Text color={theme.muted}>{subtitle}</Text>
        </Box>
      )}
    </Box>
  );
}
