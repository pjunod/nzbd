import { useMemo } from 'react';
import { Pressable, StyleSheet, Text, View } from 'react-native';

import { useTheme, useThemePreference } from '../theme';
import type { Theme, ThemePreference } from '../theme';

const OPTIONS: { label: string; value: ThemePreference }[] = [
  { label: 'System', value: 'system' },
  { label: 'Light', value: 'light' },
  { label: 'Dark', value: 'dark' },
];

export function ThemeSwitcher({ compact = false }: { compact?: boolean }) {
  const theme = useTheme();
  const styles = useMemo(() => makeStyles(theme), [theme]);
  const { preference, setPreference } = useThemePreference();

  return (
    <View accessibilityLabel="Color theme" style={[styles.group, compact && styles.groupCompact]}>
      {OPTIONS.map((option) => {
        const selected = preference === option.value;
        return (
          <Pressable
            accessibilityRole="button"
            accessibilityState={{ selected }}
            key={option.value}
            onPress={() => setPreference(option.value)}
            style={({ pressed }) => [
              styles.option,
              compact && styles.optionCompact,
              selected && styles.optionSelected,
              pressed && styles.pressed,
            ]}
          >
            <Text
              style={[
                styles.label,
                compact && styles.labelCompact,
                selected && styles.labelSelected,
              ]}
            >
              {option.label}
            </Text>
          </Pressable>
        );
      })}
    </View>
  );
}

const makeStyles = (theme: Theme) =>
  StyleSheet.create({
    group: {
      flexDirection: 'row',
      alignSelf: 'flex-start',
      padding: 3,
      borderRadius: 11,
      borderWidth: 1,
      borderColor: theme.border,
      backgroundColor: theme.panelAlt,
    },
    groupCompact: { borderRadius: 9, padding: 2 },
    option: {
      minHeight: 34,
      minWidth: 66,
      paddingHorizontal: 10,
      borderRadius: 8,
      alignItems: 'center',
      justifyContent: 'center',
    },
    optionCompact: { minHeight: 30, minWidth: 48, paddingHorizontal: 7, borderRadius: 7 },
    optionSelected: { backgroundColor: theme.panel },
    label: { color: theme.textMuted, fontSize: 12, fontWeight: '700' },
    labelCompact: { fontSize: 10 },
    labelSelected: { color: theme.accent },
    pressed: { opacity: 0.68 },
  });
