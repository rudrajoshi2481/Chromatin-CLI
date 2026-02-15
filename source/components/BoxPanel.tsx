import React, { type ReactNode } from 'react';
import { Box, Text } from 'ink';
import { theme } from '../theme/colors.js';
import { useTerminalSize } from '../hooks/use-terminal-size.js';

interface BoxPanelProps {
  title?: string;
  titleColor?: string;
  children: ReactNode;
  width?: number | string;
  padding?: number;
}

export default function BoxPanel({ title, titleColor, children, width, padding = 1 }: BoxPanelProps) {
  const { columns } = useTerminalSize();
  const maxWidth = width ?? columns;

  return (
    <Box
      flexDirection="column"
      borderStyle="round"
      borderColor={theme.border}
      paddingX={padding}
      width={maxWidth as any}
      overflowX="hidden"
    >
      {title && (
        <Box marginBottom={0}>
          <Text color={(titleColor as string) || theme.blue} bold wrap="truncate">{title}</Text>
        </Box>
      )}
      {children}
    </Box>
  );
}
