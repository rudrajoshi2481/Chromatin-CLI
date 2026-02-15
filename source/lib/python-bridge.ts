/**
 * Python IPC Bridge
 * Spawns the Python backend and communicates via JSON over stdin/stdout.
 */

import { spawn, ChildProcess } from 'node:child_process';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const PROJECT_ROOT = resolve(__dirname, '..', '..');
const IPC_SCRIPT = resolve(PROJECT_ROOT, 'src', 'python', 'ipc.py');

export interface IPCResponse {
  ok: boolean;
  data?: any;
  error?: string;
  traceback?: string;
}

/**
 * Execute a single Python IPC command (one-shot).
 */
export async function pythonCall(action: string, params: Record<string, any> = {}): Promise<IPCResponse> {
  return new Promise((resolve, reject) => {
    const child = spawn('python3', ['-m', 'src.python.ipc', action, JSON.stringify(params)], {
      cwd: PROJECT_ROOT,
      env: { ...process.env },
      stdio: ['pipe', 'pipe', 'pipe'],
    });

    let stdout = '';
    let stderr = '';

    child.stdout.on('data', (data: Buffer) => {
      stdout += data.toString();
    });

    child.stderr.on('data', (data: Buffer) => {
      stderr += data.toString();
    });

    child.on('close', (code) => {
      try {
        // Try to parse the last complete JSON line from stdout
        const lines = stdout.trim().split('\n').filter(l => l.trim());
        const lastLine = lines[lines.length - 1] || '{}';
        const result = JSON.parse(lastLine);
        resolve(result as IPCResponse);
      } catch (e) {
        resolve({
          ok: false,
          error: `Python process exited with code ${code}. stderr: ${stderr.slice(-500)}. stdout: ${stdout.slice(-500)}`,
        });
      }
    });

    child.on('error', (err) => {
      resolve({ ok: false, error: `Failed to spawn Python: ${err.message}` });
    });
  });
}

/**
 * Persistent Python IPC connection for streaming.
 */
export class PythonBridge {
  private child: ChildProcess | null = null;
  private buffer = '';
  private pending: Map<number, { resolve: (v: IPCResponse) => void; reject: (e: Error) => void }> = new Map();
  private nextId = 1;
  private stderrCallback?: (data: string) => void;

  constructor(private onStderr?: (data: string) => void) {
    this.stderrCallback = onStderr;
  }

  start(): void {
    this.child = spawn('python3', ['-m', 'src.python.ipc'], {
      cwd: PROJECT_ROOT,
      env: { ...process.env },
      stdio: ['pipe', 'pipe', 'pipe'],
    });

    this.child.stdout!.on('data', (data: Buffer) => {
      this.buffer += data.toString();
      this._processBuffer();
    });

    this.child.stderr!.on('data', (data: Buffer) => {
      if (this.stderrCallback) {
        this.stderrCallback(data.toString());
      }
    });

    this.child.on('close', () => {
      for (const [, { reject }] of this.pending) {
        reject(new Error('Python process closed'));
      }
      this.pending.clear();
    });
  }

  private _processBuffer(): void {
    const lines = this.buffer.split('\n');
    this.buffer = lines.pop() || '';

    for (const line of lines) {
      if (!line.trim()) continue;
      try {
        const result = JSON.parse(line);
        // Resolve the oldest pending request
        const firstKey = this.pending.keys().next().value;
        if (firstKey !== undefined) {
          const handler = this.pending.get(firstKey);
          this.pending.delete(firstKey);
          handler?.resolve(result);
        }
      } catch {
        // Ignore non-JSON lines
      }
    }
  }

  async call(action: string, params: Record<string, any> = {}): Promise<IPCResponse> {
    if (!this.child) {
      this.start();
    }

    return new Promise((resolve, reject) => {
      const id = this.nextId++;
      this.pending.set(id, { resolve, reject });

      const msg = JSON.stringify({ action, params }) + '\n';
      this.child!.stdin!.write(msg);
    });
  }

  stop(): void {
    if (this.child) {
      this.child.stdin!.end();
      this.child.kill();
      this.child = null;
    }
  }
}
