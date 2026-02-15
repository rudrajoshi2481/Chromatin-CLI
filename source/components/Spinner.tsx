import React from 'react';
import { Box, Text } from 'ink';
import InkSpinner from 'ink-spinner';
import { theme } from '../theme/colors.js';

interface SpinnerProps {
  label?: string;
}

export default function Spinner({ label = 'Loading...' }: SpinnerProps) {
  return (
    <Box>
      <Text color={theme.cyan}>
        <InkSpinner type="dots" />
      </Text>
      <Text color={theme.muted}> {label}</Text>
    </Box>
  );
}
