/**
 * React hook for calling the Python backend
 */

import { useState, useCallback } from 'react';
import { pythonCall, type IPCResponse } from '../lib/python-bridge.js';

export interface UsePythonResult<T = any> {
  data: T | null;
  loading: boolean;
  error: string | null;
  call: (action: string, params?: Record<string, any>) => Promise<T | null>;
  reset: () => void;
}

export function usePython<T = any>(): UsePythonResult<T> {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const call = useCallback(async (action: string, params: Record<string, any> = {}): Promise<T | null> => {
    setLoading(true);
    setError(null);

    try {
      const result: IPCResponse = await pythonCall(action, params);
      if (result.ok) {
        setData(result.data as T);
        setLoading(false);
        return result.data as T;
      } else {
        setError(result.error || 'Unknown error');
        setLoading(false);
        return null;
      }
    } catch (e: any) {
      setError(e.message || 'Failed to call Python backend');
      setLoading(false);
      return null;
    }
  }, []);

  const reset = useCallback(() => {
    setData(null);
    setLoading(false);
    setError(null);
  }, []);

  return { data, loading, error, call, reset };
}
