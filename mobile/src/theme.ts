import {
  createContext,
  createElement,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import type { PropsWithChildren } from 'react';
import { useColorScheme } from 'react-native';
import type { ColorSchemeName } from 'react-native';

import {
  loadLayoutPreference,
  loadPalettePreference,
  loadThemePreference,
  saveLayoutPreference,
  savePalettePreference,
  saveThemePreference,
} from './storage/themePreference';

export type ThemePreference = 'system' | 'light' | 'dark';
export type LayoutPreference = 'classic' | 'plex' | 'theater';
export type PalettePreference =
  | 'classic'
  | 'terminal'
  | 'noirr'
  | 'amber'
  | 'giallo'
  | 'silver'
  | 'void'
  | 'vhs'
  | 'paper'
  | 'tide'
  | 'panoptic'
  | 'redline'
  | 'panovic';

export const LAYOUT_OPTIONS: ReadonlyArray<{ id: LayoutPreference; name: string }> = [
  { id: 'classic', name: 'Classic' },
  { id: 'plex', name: 'Plex' },
  { id: 'theater', name: 'Theater' },
];

export const PALETTE_OPTIONS: ReadonlyArray<{ id: PalettePreference; name: string; darkOnly?: boolean }> = [
  { id: 'classic', name: 'Classic' },
  { id: 'terminal', name: 'Terminal' },
  { id: 'noirr', name: 'noirr' },
  { id: 'amber', name: 'Amber' },
  { id: 'giallo', name: 'Giallo' },
  { id: 'silver', name: 'Silver' },
  { id: 'void', name: 'Void', darkOnly: true },
  { id: 'vhs', name: 'VHS', darkOnly: true },
  { id: 'paper', name: 'Paper' },
  { id: 'tide', name: 'Tide' },
  { id: 'panoptic', name: 'Panoptic', darkOnly: true },
  { id: 'redline', name: 'Redline', darkOnly: true },
  { id: 'panovic', name: 'Panovic', darkOnly: true },
];

export interface Theme {
  dark: boolean;
  background: string;
  panel: string;
  panelAlt: string;
  text: string;
  textMuted: string;
  border: string;
  accent: string;
  onAccent: string;
  accentSoft: string;
  success: string;
  warning: string;
  danger: string;
  dangerSoft: string;
  overlay: string;
}

type ThemeColors = Omit<Theme, 'dark' | 'accentSoft' | 'dangerSoft' | 'overlay'>;
type PaletteThemes = { dark: Theme; light?: Theme };

function alpha(hex: string, opacity: number): string {
  const value = hex.replace('#', '');
  const red = Number.parseInt(value.slice(0, 2), 16);
  const green = Number.parseInt(value.slice(2, 4), 16);
  const blue = Number.parseInt(value.slice(4, 6), 16);
  return `rgba(${red}, ${green}, ${blue}, ${opacity})`;
}

function makeTheme(dark: boolean, colors: ThemeColors): Theme {
  return {
    dark,
    ...colors,
    accentSoft: alpha(colors.accent, dark ? 0.16 : 0.14),
    dangerSoft: alpha(colors.danger, dark ? 0.18 : 0.14),
    overlay: dark ? 'rgba(0, 0, 0, 0.68)' : 'rgba(12, 26, 40, 0.45)',
  };
}

const palettes: Record<PalettePreference, PaletteThemes> = {
  classic: {
    dark: {
      dark: true, background: '#0B1219', panel: '#121C25', panelAlt: '#1A2834',
      text: '#EDF5FA', textMuted: '#92A5B5', border: '#2A3A47', accent: '#55AFFF',
      onAccent: '#FFFFFF', accentSoft: '#163B5B', success: '#4ED29D', warning: '#F3B95F',
      danger: '#FF7D86', dangerSoft: '#4A2329', overlay: 'rgba(0, 0, 0, 0.68)',
    },
    light: {
      dark: false, background: '#F2F5F8', panel: '#FFFFFF', panelAlt: '#EAF0F5',
      text: '#132231', textMuted: '#617181', border: '#D7E0E8', accent: '#0B77D5',
      onAccent: '#FFFFFF', accentSoft: '#DCEEFF', success: '#17865D', warning: '#B86D08',
      danger: '#C13D48', dangerSoft: '#FBE6E8', overlay: 'rgba(12, 26, 40, 0.45)',
    },
  },
  terminal: {
    dark: makeTheme(true, {
      background: '#050705', panel: '#0b100b', panelAlt: '#111a12', border: '#1e3020',
      text: '#c9e8c0', textMuted: '#7a9670', accent: '#3fe170', onAccent: '#041508',
      success: '#3fe170', warning: '#d9c25a', danger: '#ff7066',
    }),
    light: makeTheme(false, {
      background: '#eee8d5', panel: '#fdf6e3', panelAlt: '#e6dfc8', border: '#d5cdb4',
      text: '#073642', textMuted: '#657b83', accent: '#6e7f00', onAccent: '#fdf6e3',
      success: '#5c6900', warning: '#7d5e00', danger: '#ba2a27',
    }),
  },
  noirr: {
    dark: makeTheme(true, {
      background: '#0a0a0c', panel: '#101014', panelAlt: '#16161b', border: '#242429',
      text: '#ededef', textMuted: '#9a9aa3', accent: '#e5484d', onAccent: '#ffffff',
      success: '#5fb582', warning: '#d9a05b', danger: '#ff7a66',
    }),
    light: makeTheme(false, {
      background: '#f2efe8', panel: '#faf8f2', panelAlt: '#ffffff', border: '#d8d5cf',
      text: '#1a1a1e', textMuted: '#5d5c63', accent: '#c2343a', onAccent: '#ffffff',
      success: '#307a54', warning: '#8d6425', danger: '#aa5438',
    }),
  },
  amber: {
    dark: makeTheme(true, {
      background: '#191a1d', panel: '#212327', panelAlt: '#2a2d32', border: '#383c43',
      text: '#eceef0', textMuted: '#9aa0a7', accent: '#e5a00d', onAccent: '#1c1303',
      success: '#52b788', warning: '#f2c14e', danger: '#ee7168',
    }),
    light: makeTheme(false, {
      background: '#f3f4f6', panel: '#ffffff', panelAlt: '#e9ebee', border: '#d5d9de',
      text: '#1e2124', textMuted: '#5f666d', accent: '#8b5e00', onAccent: '#ffffff',
      success: '#246b49', warning: '#8a6116', danger: '#bd332d',
    }),
  },
  giallo: {
    dark: makeTheme(true, {
      background: '#0c0a06', panel: '#14100a', panelAlt: '#1c160d', border: '#332a1d',
      text: '#f2e9d8', textMuted: '#a89c85', accent: '#e8a33d', onAccent: '#1a1002',
      success: '#5fb582', warning: '#c9723a', danger: '#e5484d',
    }),
    light: makeTheme(false, {
      background: '#f5eed9', panel: '#fbf7ea', panelAlt: '#ffffff', border: '#d8cdb2',
      text: '#241d10', textMuted: '#6e6350', accent: '#7f4e00', onAccent: '#ffffff',
      success: '#2c734d', warning: '#8a6116', danger: '#b23a35',
    }),
  },
  silver: {
    dark: makeTheme(true, {
      background: '#0a0a0b', panel: '#131315', panelAlt: '#1b1b1e', border: '#29292c',
      text: '#f2f2f2', textMuted: '#9a9a9e', accent: '#e8e8ea', onAccent: '#0a0a0b',
      success: '#9fbfa8', warning: '#c9b48c', danger: '#d09088',
    }),
    light: makeTheme(false, {
      background: '#f4f4f2', panel: '#ffffff', panelAlt: '#eaeae7', border: '#d5d5d1',
      text: '#141416', textMuted: '#66666a', accent: '#1a1a1c', onAccent: '#ffffff',
      success: '#467353', warning: '#765f28', danger: '#a05248',
    }),
  },
  void: {
    dark: makeTheme(true, {
      background: '#000000', panel: '#0a0a0a', panelAlt: '#131313', border: '#292929',
      text: '#e8e8e8', textMuted: '#8a8a8a', accent: '#4cc2ff', onAccent: '#001018',
      success: '#34d399', warning: '#fbbf24', danger: '#f87171',
    }),
  },
  vhs: {
    dark: makeTheme(true, {
      background: '#140d22', panel: '#1d1430', panelAlt: '#291c42', border: '#492647',
      text: '#f4e9ff', textMuted: '#a78fc7', accent: '#ff4fd8', onAccent: '#22041c',
      success: '#3ddc97', warning: '#ffb454', danger: '#ff5c7a',
    }),
  },
  paper: {
    dark: makeTheme(true, {
      background: '#171715', panel: '#201f1c', panelAlt: '#2a2823', border: '#454139',
      text: '#f2eee6', textMuted: '#aaa49a', accent: '#9db2ff', onAccent: '#101322',
      success: '#6fc49a', warning: '#e2c36f', danger: '#ff8a80',
    }),
    light: makeTheme(false, {
      background: '#f4f0e8', panel: '#fffdf8', panelAlt: '#e9e2d7', border: '#c8bfb2',
      text: '#1f2328', textMuted: '#5f625f', accent: '#3451b2', onAccent: '#ffffff',
      success: '#26724c', warning: '#765c00', danger: '#b3261e',
    }),
  },
  tide: {
    dark: makeTheme(true, {
      background: '#071412', panel: '#0d1e1b', panelAlt: '#142a25', border: '#2a4841',
      text: '#e4f2ed', textMuted: '#93aaa2', accent: '#73d6b1', onAccent: '#052019',
      success: '#6fd39f', warning: '#e5c875', danger: '#ff8a80',
    }),
    light: makeTheme(false, {
      background: '#edf4f0', panel: '#fbfdfa', panelAlt: '#dfeae4', border: '#bccfc5',
      text: '#14201c', textMuted: '#586a62', accent: '#176b54', onAccent: '#ffffff',
      success: '#196b48', warning: '#735b0b', danger: '#b33a32',
    }),
  },
  panoptic: {
    dark: makeTheme(true, {
      background: '#0a0a0f', panel: '#10131b', panelAlt: '#171c25', border: '#293e48',
      text: '#e8eaed', textMuted: '#9eaab2', accent: '#00d4ff', onAccent: '#001014',
      success: '#5ce1b4', warning: '#ffd178', danger: '#ff8191',
    }),
  },
  redline: {
    dark: makeTheme(true, {
      background: '#070708', panel: '#111214', panelAlt: '#191a1d', border: '#44282c',
      text: '#f0eded', textMuted: '#aaa1a3', accent: '#ff5964', onAccent: '#170204',
      success: '#6ccf9a', warning: '#f6c760', danger: '#ff9f70',
    }),
  },
  panovic: {
    dark: makeTheme(true, {
      background: '#000000', panel: '#181818', panelAlt: '#242424', border: 'rgba(255, 255, 255, 0.06)',
      text: '#e8e6e1', textMuted: '#9a9aa0', accent: '#f0723b', onAccent: '#140a00',
      success: '#5fb582', warning: '#d9a05b', danger: '#ff7a66',
    }),
  },
};

interface ThemeContextValue {
  theme: Theme;
  preference: ThemePreference;
  layout: LayoutPreference;
  palette: PalettePreference;
  setPreference: (preference: ThemePreference) => void;
  setLayout: (layout: LayoutPreference) => void;
  setPalette: (palette: PalettePreference) => void;
}

const ThemeContext = createContext<ThemeContextValue | null>(null);

export function ThemeProvider({ children }: PropsWithChildren) {
  const systemScheme = useColorScheme();
  const [preference, setPreferenceState] = useState<ThemePreference>('system');
  const [layout, setLayoutState] = useState<LayoutPreference>('classic');
  const [palette, setPaletteState] = useState<PalettePreference>('classic');
  const changed = useRef({ preference: false, layout: false, palette: false });

  useEffect(() => {
    let active = true;
    void Promise.all([loadThemePreference(), loadLayoutPreference(), loadPalettePreference()]).then(([storedTheme, storedLayout, storedPalette]) => {
      if (!active) return;
      if (storedTheme && !changed.current.preference) setPreferenceState(storedTheme);
      if (storedLayout && !changed.current.layout) setLayoutState(storedLayout);
      if (storedPalette && !changed.current.palette) setPaletteState(storedPalette);
    });
    return () => {
      active = false;
    };
  }, []);

  const setPreference = useCallback((next: ThemePreference) => {
    changed.current.preference = true;
    setPreferenceState(next);
    void saveThemePreference(next).catch(() => undefined);
  }, []);
  const setLayout = useCallback((next: LayoutPreference) => {
    changed.current.layout = true;
    setLayoutState(next);
    void saveLayoutPreference(next).catch(() => undefined);
  }, []);
  const setPalette = useCallback((next: PalettePreference) => {
    changed.current.palette = true;
    setPaletteState(next);
    void savePalettePreference(next).catch(() => undefined);
  }, []);
  const value = useMemo(
    () => ({
      theme: resolveTheme(preference, systemScheme, palette),
      preference,
      layout,
      palette,
      setPreference,
      setLayout,
      setPalette,
    }),
    [layout, palette, preference, setLayout, setPalette, setPreference, systemScheme],
  );

  return createElement(ThemeContext.Provider, { value }, children);
}

export function isDarkOnlyPalette(palette: PalettePreference): boolean {
  return PALETTE_OPTIONS.find((option) => option.id === palette)?.darkOnly === true;
}

export function resolveTheme(
  preference: ThemePreference,
  systemScheme: ColorSchemeName | null | undefined,
  palette: PalettePreference = 'classic',
): Theme {
  const choice = palettes[palette];
  if (isDarkOnlyPalette(palette)) return choice.dark;
  if (preference === 'light') return choice.light ?? choice.dark;
  if (preference === 'dark') return choice.dark;
  return systemScheme === 'light' ? choice.light ?? choice.dark : choice.dark;
}

export function useTheme(): Theme {
  return useThemeContext().theme;
}

export function useThemePreference(): Pick<ThemeContextValue, 'preference' | 'setPreference'> {
  const { preference, setPreference } = useThemeContext();
  return { preference, setPreference };
}

export function useDisplayPreferences(): Omit<ThemeContextValue, 'theme'> {
  const { preference, layout, palette, setPreference, setLayout, setPalette } = useThemeContext();
  return { preference, layout, palette, setPreference, setLayout, setPalette };
}

function useThemeContext(): ThemeContextValue {
  const context = useContext(ThemeContext);
  if (!context) throw new Error('Theme hooks must be used inside ThemeProvider.');
  return context;
}
