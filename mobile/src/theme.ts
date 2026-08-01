import {
  createContext,
  createElement,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from 'react';
import type { PropsWithChildren } from 'react';
import { useColorScheme } from 'react-native';
import type { ColorSchemeName } from 'react-native';

import { loadThemePreference, saveThemePreference } from './storage/themePreference';

export type ThemePreference = 'system' | 'light' | 'dark';

export interface Theme {
  dark: boolean;
  background: string;
  panel: string;
  panelAlt: string;
  text: string;
  textMuted: string;
  border: string;
  accent: string;
  accentSoft: string;
  success: string;
  warning: string;
  danger: string;
  dangerSoft: string;
  overlay: string;
}

const light: Theme = {
  dark: false,
  background: '#F2F5F8',
  panel: '#FFFFFF',
  panelAlt: '#EAF0F5',
  text: '#132231',
  textMuted: '#617181',
  border: '#D7E0E8',
  accent: '#0B77D5',
  accentSoft: '#DCEEFF',
  success: '#17865D',
  warning: '#B86D08',
  danger: '#C13D48',
  dangerSoft: '#FBE6E8',
  overlay: 'rgba(12, 26, 40, 0.45)',
};

const dark: Theme = {
  dark: true,
  background: '#0B1219',
  panel: '#121C25',
  panelAlt: '#1A2834',
  text: '#EDF5FA',
  textMuted: '#92A5B5',
  border: '#2A3A47',
  accent: '#55AFFF',
  accentSoft: '#163B5B',
  success: '#4ED29D',
  warning: '#F3B95F',
  danger: '#FF7D86',
  dangerSoft: '#4A2329',
  overlay: 'rgba(0, 0, 0, 0.68)',
};

interface ThemeContextValue {
  theme: Theme;
  preference: ThemePreference;
  setPreference: (preference: ThemePreference) => void;
}

const ThemeContext = createContext<ThemeContextValue | null>(null);

export function ThemeProvider({ children }: PropsWithChildren) {
  const systemScheme = useColorScheme();
  const [preference, setPreferenceState] = useState<ThemePreference>('system');

  useEffect(() => {
    let active = true;
    void loadThemePreference().then((stored) => {
      if (active && stored) setPreferenceState(stored);
    });
    return () => {
      active = false;
    };
  }, []);

  const setPreference = useCallback((next: ThemePreference) => {
    setPreferenceState(next);
    void saveThemePreference(next).catch(() => undefined);
  }, []);
  const value = useMemo(
    () => ({ theme: resolveTheme(preference, systemScheme), preference, setPreference }),
    [preference, setPreference, systemScheme],
  );

  return createElement(ThemeContext.Provider, { value }, children);
}

export function resolveTheme(
  preference: ThemePreference,
  systemScheme: ColorSchemeName | null | undefined,
): Theme {
  if (preference === 'light') return light;
  if (preference === 'dark') return dark;
  return systemScheme === 'light' ? light : dark;
}

export function useTheme(): Theme {
  return useThemeContext().theme;
}

export function useThemePreference(): Pick<ThemeContextValue, 'preference' | 'setPreference'> {
  const { preference, setPreference } = useThemeContext();
  return { preference, setPreference };
}

function useThemeContext(): ThemeContextValue {
  const context = useContext(ThemeContext);
  if (!context) throw new Error('Theme hooks must be used inside ThemeProvider.');
  return context;
}
