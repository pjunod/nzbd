import Constants from 'expo-constants';
import { Platform } from 'react-native';

import { formatAppVersion } from './app-version-format';

const nativeBuild = Platform.select({
  ios: Constants.platform?.ios?.buildNumber,
  android: Constants.platform?.android?.versionCode?.toString(),
});

export const APP_VERSION = formatAppVersion(Constants.expoConfig?.version, nativeBuild);
