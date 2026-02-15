import React, { useEffect, useState } from 'react';
import { Box, Text, useInput } from 'ink';
import { theme, icons } from '../theme/colors.js';
import { usePython } from '../hooks/use-python.js';
import BoxPanel from './BoxPanel.js';
import Spinner from './Spinner.js';

interface FetchDataProps {
  onDone: () => void;
}

export default function FetchData({ onDone }: FetchDataProps) {
  const fetcher = usePython();
  const [phase, setPhase] = useState<'fetching' | 'done' | 'error'>('fetching');
  const [mcoolCount, setMcoolCount] = useState(0);
  const [ccreFound, setCcreFound] = useState(0);
  const [ccreTotal, setCcreTotal] = useState(0);

  useInput((input, key) => {
    if (phase === 'done' || phase === 'error') {
      if (key.escape || input === 'q' || key.return) {
        onDone();
      }
    }
  });

  useEffect(() => {
    fetcher.call('fetch_all_data', { force_refresh: true }).then((result) => {
      if (result) {
        setMcoolCount(result.mcool_count || 0);
        setCcreFound(result.ccre_found || 0);
        setCcreTotal(result.ccre_total || 0);
        setPhase('done');
      } else {
        setPhase('error');
      }
    });
  }, []);

  useEffect(() => {
    if (phase === 'done') {
      const timer = setTimeout(onDone, 2500);
      return () => clearTimeout(timer);
    }
  }, [phase]);

  return (
    <BoxPanel title={`${icons.download} Fetching Data from APIs`} titleColor={theme.cyan}>
      <Box flexDirection="column">
        {phase === 'fetching' && (
          <Box flexDirection="column">
            <Spinner label="Fetching .mcool files from 4DN + cCRE from ENCODE..." />
            <Text color={theme.muted}>This may take a few minutes (querying ENCODE for each cell line)</Text>
          </Box>
        )}

        {phase === 'done' && (
          <Box flexDirection="column">
            <Text color={theme.success}>
              {icons.check} Fetched {mcoolCount} .mcool files from 4DN
            </Text>
            <Text color={theme.success}>
              {icons.check} Found cCRE data for {ccreFound}/{ccreTotal} cell lines from ENCODE
            </Text>
            <Box marginTop={1}>
              <Text color={theme.success} bold>
                {icons.check} Database populated! Returning to menu...
              </Text>
            </Box>
            <Text color={theme.muted}>Press Enter or q to continue</Text>
          </Box>
        )}

        {phase === 'error' && (
          <Box flexDirection="column">
            <Text color={theme.error}>{icons.cross} Error: {fetcher.error || 'Unknown error'}</Text>
            <Text color={theme.muted}>Press q to go back</Text>
          </Box>
        )}
      </Box>
    </BoxPanel>
  );
}
