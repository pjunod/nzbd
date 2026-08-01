import { useColorScheme } from 'react-native';

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

export function useTheme(): Theme {
  return useColorScheme() === 'dark' ? dark : light;
}
