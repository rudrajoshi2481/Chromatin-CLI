/**
 * Formatting utilities
 */

export function sizeFormat(bytes: number): string {
  if (!bytes || bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let val = Math.abs(bytes);
  let i = 0;
  while (val >= 1024 && i < units.length - 1) {
    val /= 1024;
    i++;
  }
  return `${val.toFixed(1)} ${units[i]}`;
}

export function gbFormat(gb: number): string {
  if (gb < 0.01) return '< 0.01 GB';
  return `${gb.toFixed(2)} GB`;
}

export function mbFormat(mb: number): string {
  if (mb < 0.01) return '< 0.01 MB';
  return `${mb.toFixed(1)} MB`;
}

export function padRight(str: string, len: number): string {
  return str.length >= len ? str.slice(0, len) : str + ' '.repeat(len - str.length);
}

export function padLeft(str: string, len: number): string {
  return str.length >= len ? str.slice(0, len) : ' '.repeat(len - str.length) + str;
}

export function truncate(str: string, len: number): string {
  if (str.length <= len) return str;
  return str.slice(0, len - 1) + '…';
}

export function horizontalLine(width: number, char = '─'): string {
  return char.repeat(width);
}

export function barChart(value: number, max: number, width: number): string {
  const filled = Math.round((value / max) * width);
  const empty = width - filled;
  return '█'.repeat(Math.max(0, filled)) + '░'.repeat(Math.max(0, empty));
}

export function sparkline(values: number[]): string {
  if (values.length === 0) return '';
  const chars = '▁▂▃▄▅▆▇█';
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  return values.map(v => {
    const idx = Math.round(((v - min) / range) * (chars.length - 1));
    return chars[idx];
  }).join('');
}
