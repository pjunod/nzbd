import { useMemo, useState } from 'react';
import { Modal, Pressable, ScrollView, StyleSheet, Text, View } from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';

import {
  LAYOUT_OPTIONS,
  PALETTE_OPTIONS,
  useDisplayPreferences,
  useTheme,
} from '../theme';
import type {
  Theme,
  ThemePreference,
} from '../theme';

const APPEARANCE_OPTIONS: ReadonlyArray<{ id: ThemePreference; name: string }> = [
  { id: 'system', name: 'Auto' },
  { id: 'light', name: 'Light' },
  { id: 'dark', name: 'Dark' },
];

export function ThemeSwitcher({ compact = false }: { compact?: boolean }) {
  const theme = useTheme();
  const styles = useMemo(() => makeStyles(theme), [theme]);
  const [open, setOpen] = useState(false);
  const {
    preference,
    layout,
    palette,
    setPreference,
    setLayout,
    setPalette,
  } = useDisplayPreferences();

  return (
    <>
      <Pressable
        accessibilityLabel="Display settings"
        accessibilityRole="button"
        onPress={() => setOpen(true)}
        style={({ pressed }) => [
          styles.trigger,
          compact && styles.triggerCompact,
          pressed && styles.pressed,
        ]}
      >
        <View style={styles.swatch} />
        <Text style={[styles.triggerLabel, compact && styles.triggerLabelCompact]}>Display</Text>
      </Pressable>

      <Modal animationType="fade" onRequestClose={() => setOpen(false)} transparent visible={open}>
        <View style={styles.modal}>
          <Pressable accessibilityLabel="Close display settings" accessibilityRole="button" onPress={() => setOpen(false)} style={StyleSheet.absoluteFill} />
          <SafeAreaView edges={['bottom', 'left', 'right']} style={styles.sheet}>
            <View style={styles.sheetHeader}>
              <View>
                <Text style={styles.title}>Display</Text>
                <Text style={styles.subtitle}>Layout, color scheme, and appearance are independent.</Text>
              </View>
              <Pressable accessibilityRole="button" onPress={() => setOpen(false)} style={styles.doneButton}>
                <Text style={styles.done}>Done</Text>
              </Pressable>
            </View>

            <ScrollView contentContainerStyle={styles.sheetContent} showsVerticalScrollIndicator={false} style={styles.sheetScroll}>
              <ChoiceSection title="Layout" hint="Classic is the original native interface.">
                {LAYOUT_OPTIONS.map((option) => (
                  <Choice
                    key={option.id}
                    label={option.name}
                    onPress={() => setLayout(option.id)}
                    selected={layout === option.id}
                    styles={styles}
                  />
                ))}
              </ChoiceSection>

              <ChoiceSection title="Color scheme" hint="Void and VHS are midnight-only.">
                {PALETTE_OPTIONS.map((option) => (
                  <Choice
                    key={option.id}
                    label={option.name}
                    onPress={() => setPalette(option.id)}
                    selected={palette === option.id}
                    styles={styles}
                  />
                ))}
              </ChoiceSection>

              <ChoiceSection title="Appearance" hint="Auto follows this device and falls back to dark.">
                {APPEARANCE_OPTIONS.map((option) => (
                  <Choice
                    key={option.id}
                    label={option.name}
                    onPress={() => setPreference(option.id)}
                    selected={preference === option.id}
                    styles={styles}
                  />
                ))}
              </ChoiceSection>
            </ScrollView>
          </SafeAreaView>
        </View>
      </Modal>
    </>
  );
}

function ChoiceSection({
  title,
  hint,
  children,
}: {
  title: string;
  hint: string;
  children: React.ReactNode;
}) {
  const theme = useTheme();
  const styles = useMemo(() => makeStyles(theme), [theme]);
  return (
    <View style={styles.section}>
      <Text style={styles.sectionTitle}>{title}</Text>
      <Text style={styles.hint}>{hint}</Text>
      <View style={styles.choices}>{children}</View>
    </View>
  );
}

function Choice({
  label,
  selected,
  onPress,
  styles,
}: {
  label: string;
  selected: boolean;
  onPress: () => void;
  styles: ReturnType<typeof makeStyles>;
}) {
  return (
    <Pressable
      accessibilityRole="button"
      accessibilityState={{ selected }}
      onPress={onPress}
      style={({ pressed }) => [
        styles.option,
        selected && styles.optionSelected,
        pressed && styles.pressed,
      ]}
    >
      <Text style={[styles.optionLabel, selected && styles.optionLabelSelected]}>{label}</Text>
    </Pressable>
  );
}

const makeStyles = (theme: Theme) =>
  StyleSheet.create({
    trigger: {
      minHeight: 38,
      paddingHorizontal: 11,
      borderRadius: 10,
      borderWidth: 1,
      borderColor: theme.border,
      backgroundColor: theme.panelAlt,
      flexDirection: 'row',
      alignItems: 'center',
      justifyContent: 'center',
      gap: 6,
    },
    triggerCompact: { minHeight: 34, paddingHorizontal: 8, borderRadius: 9 },
    swatch: { width: 9, height: 9, borderRadius: 5, backgroundColor: theme.accent },
    triggerLabel: { color: theme.text, fontSize: 12, fontWeight: '800' },
    triggerLabelCompact: { fontSize: 10 },
    modal: { flex: 1, justifyContent: 'flex-end', backgroundColor: theme.overlay },
    sheet: {
      maxHeight: '88%',
      padding: 18,
      borderTopLeftRadius: 24,
      borderTopRightRadius: 24,
      borderWidth: 1,
      borderBottomWidth: 0,
      borderColor: theme.border,
      backgroundColor: theme.panel,
      gap: 18,
    },
    sheetHeader: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'flex-start', gap: 16 },
    sheetScroll: { flexShrink: 1 },
    sheetContent: { gap: 18, paddingBottom: 8 },
    title: { color: theme.text, fontSize: 24, fontWeight: '900' },
    subtitle: { color: theme.textMuted, fontSize: 11, lineHeight: 16, marginTop: 3 },
    doneButton: { minHeight: 38, justifyContent: 'center', paddingHorizontal: 6 },
    done: { color: theme.accent, fontSize: 14, fontWeight: '800' },
    section: { gap: 8 },
    sectionTitle: { color: theme.text, fontSize: 14, fontWeight: '800' },
    hint: { color: theme.textMuted, fontSize: 11, lineHeight: 15 },
    choices: { flexDirection: 'row', flexWrap: 'wrap', gap: 7 },
    option: {
      minHeight: 36,
      paddingHorizontal: 12,
      borderRadius: 18,
      borderWidth: 1,
      borderColor: theme.border,
      backgroundColor: theme.panelAlt,
      alignItems: 'center',
      justifyContent: 'center',
    },
    optionSelected: { borderColor: theme.accent, backgroundColor: theme.accentSoft },
    optionLabel: { color: theme.textMuted, fontSize: 12, fontWeight: '700' },
    optionLabelSelected: { color: theme.accent },
    pressed: { opacity: 0.68 },
  });
