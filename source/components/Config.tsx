import React, { useEffect, useState } from 'react';
import { Box, Text, useInput } from 'ink';
import TextInput from 'ink-text-input';
import { theme, icons } from '../theme/colors.js';
import { usePython } from '../hooks/use-python.js';
import { useTerminalSize } from '../hooks/use-terminal-size.js';
import BoxPanel from './BoxPanel.js';
import Spinner from './Spinner.js';

interface ConfigProps {
  onBack: () => void;
}

const CONFIG_KEYS = [
  { key: 'FOURDN_ACCESS_ID', label: '4DN Access ID', secret: false, desc: '4DN Portal API key ID' },
  { key: 'FOURDN_SECRET_KEY', label: '4DN Secret Key', secret: true, desc: '4DN Portal API secret' },
  { key: 'DATA_DIR', label: 'Data Directory', secret: false, desc: 'Where downloads and DB are stored' },
  { key: 'CACHE_TTL_HOURS', label: 'Cache TTL (hours)', secret: false, desc: 'How long to cache API responses' },
  { key: 'MAX_PARALLEL_DOWNLOADS', label: 'Max Parallel Downloads', secret: false, desc: 'Concurrent download threads' },
];

export default function Config({ onBack }: ConfigProps) {
  const configLoader = usePython();
  const configUpdater = usePython();
  const metaExporter = usePython();
  const [selectedIdx, setSelectedIdx] = useState(0);
  const [editing, setEditing] = useState(false);
  const [editValue, setEditValue] = useState('');
  const [message, setMessage] = useState('');
  const { columns: termCols } = useTerminalSize();
  const sepWidth = Math.min(70, termCols - 6);

  useEffect(() => {
    configLoader.call('get_config');
  }, []);

  useInput((input, key) => {
    if (editing) return; // TextInput handles input

    if (key.escape || input === 'q') {
      onBack();
      return;
    }
    if (key.upArrow) setSelectedIdx(prev => Math.max(0, prev - 1));
    if (key.downArrow) setSelectedIdx(prev => Math.min(CONFIG_KEYS.length, prev + 1)); // +1 for export button

    if (key.return) {
      // Last item = export metadata
      if (selectedIdx === CONFIG_KEYS.length) {
        setMessage('Exporting metadata...');
        metaExporter.call('export_metadata').then((data: any) => {
          if (data) {
            setMessage(`Exported: ${data.cell_lines} cell lines, ${data.experiments} experiments, ${data.ccre} cCRE to ${data.path}`);
          }
        });
        return;
      }
      // Edit config value
      const ck = CONFIG_KEYS[selectedIdx];
      if (ck) {
        const currentVal = configLoader.data?.config?.[ck.key] || '';
        setEditValue(currentVal);
        setEditing(true);
      }
    }
  });

  function handleEditSubmit(value: string) {
    const ck = CONFIG_KEYS[selectedIdx];
    if (ck) {
      configUpdater.call('update_config', { key: ck.key, value }).then(() => {
        setMessage(`Updated ${ck.key}`);
        configLoader.call('get_config');
      });
    }
    setEditing(false);
    setEditValue('');
  }

  function handleEditCancel() {
    setEditing(false);
    setEditValue('');
  }

  if (configLoader.loading && !configLoader.data) {
    return <Spinner label="Loading config..." />;
  }

  const config = configLoader.data?.config || {};
  const envPath = configLoader.data?.path || '.env';

  return (
    <Box flexDirection="column">
      <BoxPanel title={`${icons.config} Configuration`} titleColor={theme.orange}>
        <Text color={theme.muted}>File: <Text color={theme.cyan}>{envPath}</Text></Text>
        <Text color={theme.border}>{'─'.repeat(sepWidth)}</Text>

        {CONFIG_KEYS.map((ck, i) => {
          const isCursor = i === selectedIdx;
          const val = config[ck.key] || '';
          const display = ck.secret && val ? val.slice(0, 4) + '····' : val || '(not set)';
          const isEditingThis = editing && i === selectedIdx;

          return (
            <Box key={ck.key} flexDirection="column">
              <Box>
                <Text color={isCursor ? theme.accent : theme.muted}>
                  {isCursor ? icons.arrow : ' '}{' '}
                </Text>
                <Text color={isCursor ? theme.white : theme.fg} bold={isCursor}>
                  {padR(ck.label, 24)}
                </Text>
                {isEditingThis ? (
                  <Box>
                    <Text color={theme.green}>{'> '}</Text>
                    <TextInput
                      value={editValue}
                      onChange={setEditValue}
                      onSubmit={handleEditSubmit}
                    />
                  </Box>
                ) : (
                  <Text color={val ? theme.cyan : theme.muted}>{display}</Text>
                )}
              </Box>
              {isCursor && !isEditingThis && (
                <Box marginLeft={3}>
                  <Text color={theme.muted} dimColor>{ck.desc}</Text>
                </Box>
              )}
            </Box>
          );
        })}

        <Text color={theme.border}>{'─'.repeat(sepWidth)}</Text>

        {/* Export metadata button */}
        <Box>
          <Text color={selectedIdx === CONFIG_KEYS.length ? theme.accent : theme.muted}>
            {selectedIdx === CONFIG_KEYS.length ? icons.arrow : ' '}{' '}
          </Text>
          <Text color={selectedIdx === CONFIG_KEYS.length ? theme.green : theme.fg}
                bold={selectedIdx === CONFIG_KEYS.length}>
            {icons.export_} Export Training Metadata
          </Text>
          <Text color={theme.muted}> — Save all metadata to JSON for ML pipeline</Text>
        </Box>

        {metaExporter.loading && <Spinner label="Exporting..." />}

        {message && (
          <Box marginTop={1}>
            <Text color={theme.success}>{icons.check} {message}</Text>
          </Box>
        )}

        {metaExporter.error && (
          <Box marginTop={1}>
            <Text color={theme.error}>{icons.cross} {metaExporter.error}</Text>
          </Box>
        )}

        <Box marginTop={1}>
          <Text color={theme.muted}>[</Text><Text color={theme.cyan}>↑↓</Text>
          <Text color={theme.muted}>] Nav  [</Text><Text color={theme.cyan}>Enter</Text>
          <Text color={theme.muted}>] Edit/Run  [</Text><Text color={theme.cyan}>q</Text>
          <Text color={theme.muted}>] Back</Text>
        </Box>
      </BoxPanel>
    </Box>
  );
}

function padR(s: string, n: number): string {
  return s.length >= n ? s.slice(0, n) : s + ' '.repeat(n - s.length);
}
