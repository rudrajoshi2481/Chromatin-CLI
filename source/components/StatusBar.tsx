import React from 'react';
import { Box, Text } from 'ink';
import { theme, icons } from '../theme/colors.js';

interface StatusBarProps {
  items: Array<{ label: string; value: string | number; color?: string }>;
}

export default function StatusBar({ items }: StatusBarProps) {
  return (
    <Box borderStyle="single" borderColor={theme.border} paddingX={1}>
      {items.map((item, i) => (
        <Box key={i} marginRight={2}>
          <Text color={theme.muted}>{item.label}: </Text>
          <Text color={(item.color as string) || theme.white} bold>{String(item.value)}</Text>
        </Box>
      ))}
    </Box>
  );
}
