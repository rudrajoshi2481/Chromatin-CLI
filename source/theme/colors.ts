/**
 * Tokyo Night (Darkest) Color Theme
 * Based on: https://github.com/tokyo-night/tokyo-night-vscode-theme
 */

export const theme = {
  // Core
  bg: '#1a1b26',
  fg: '#a9b1d6',
  comment: '#565f89',
  termBlack: '#414868',

  // Syntax / Accent Colors
  red: '#f7768e',
  orange: '#ff9e64',
  yellow: '#e0af68',
  lightYellow: '#cfc9c2',
  green: '#9ece6a',
  teal: '#73daca',
  cyan: '#2ac3de',
  lightCyan: '#7dcfff',
  cyanBright: '#b4f9f8',
  blue: '#7aa2f7',
  purple: '#bb9af7',
  white: '#c0caf5',

  // Semantic
  success: '#9ece6a',
  error: '#f7768e',
  warning: '#e0af68',
  info: '#7aa2f7',
  muted: '#565f89',
  accent: '#7aa2f7',
  highlight: '#bb9af7',
  border: '#414868',
  dimmed: '#565f89',

  // Status
  downloaded: '#9ece6a',
  pending: '#e0af68',
  failed: '#f7768e',
  queued: '#7dcfff',
  active: '#2ac3de',
} as const;

export type ThemeColor = keyof typeof theme;

// Box drawing characters
export const box = {
  topLeft: '╭',
  topRight: '╮',
  bottomLeft: '╰',
  bottomRight: '╯',
  horizontal: '─',
  vertical: '│',
  teeRight: '├',
  teeLeft: '┤',
  teeDown: '┬',
  teeUp: '┴',
  cross: '┼',
} as const;

// Icons — clean Unicode glyphs (no emoji, terminal-safe)
export const icons = {
  check: '✓',
  cross: '✗',
  circle: '○',
  circleFilled: '●',
  arrow: '→',
  arrowDown: '↓',
  arrowUp: '↑',
  bar: '█',
  barLight: '░',
  barMed: '▒',
  spinner: '◐',
  dna: '◈',
  chart: '▣',
  download: '↓',
  folder: '▤',
  file: '▪',
  database: '⊞',
  search: '⊙',
  warning: '△',
  info: '◇',
  star: '★',
  dot: '·',
  config: '⚙',
  play: '▶',
  stop: '■',
  pause: '‖',
  refresh: '↻',
  export_: '⇥',
  lock: '⊘',
  unlock: '⊛',
} as const;
