import { useState, useEffect } from 'react';
import { useStdout } from 'ink';

/**
 * Hook that returns terminal dimensions and updates on resize (SIGWINCH).
 * Fixes the bug where shrinking the terminal breaks the UI layout.
 */
export function useTerminalSize() {
  const { stdout } = useStdout();
  const [size, setSize] = useState({
    columns: stdout?.columns || 80,
    rows: stdout?.rows || 24,
  });

  useEffect(() => {
    function onResize() {
      if (stdout) {
        setSize({ columns: stdout.columns, rows: stdout.rows });
      }
    }
    if (stdout) {
      stdout.on('resize', onResize);
      // Sync initial
      onResize();
      return () => { stdout.off('resize', onResize); };
    }
  }, [stdout]);

  return size;
}
