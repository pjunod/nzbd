import * as ServiceDiscovery from '@inthepocket/react-native-service-discovery';
import { useCallback, useEffect, useState } from 'react';

import {
  DiscoveredNzbd,
  isNzbdServiceType,
  serviceKey,
  toDiscoveredNzbd,
} from './nzbdService';

const SERVICE_TYPE = 'nzbd';

export interface NzbdDiscoveryState {
  services: DiscoveredNzbd[];
  scanning: boolean;
  noResults: boolean;
  error: string | null;
  scanAgain: () => Promise<void>;
}

export function useNzbdDiscovery(): NzbdDiscoveryState {
  const [services, setServices] = useState<DiscoveredNzbd[]>([]);
  const [scanning, setScanning] = useState(false);
  const [noResults, setNoResults] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const start = useCallback(async () => {
    setError(null);
    setNoResults(false);
    try {
      await ServiceDiscovery.startSearch(SERVICE_TYPE);
      setScanning(true);
    } catch (cause) {
      setScanning(false);
      setError(discoveryError(cause));
    }
  }, []);

  const scanAgain = useCallback(async () => {
    setScanning(false);
    setServices([]);
    try {
      await ServiceDiscovery.stopSearch(SERVICE_TYPE);
    } catch {
      // A search that was not running is already in the desired state.
    }
    await start();
  }, [start]);

  useEffect(() => {
    let active = true;
    const found = ServiceDiscovery.addEventListener('serviceFound', (service) => {
      if (!active || !isNzbdServiceType(service.type)) {
        return;
      }
      const nzbd = toDiscoveredNzbd(service);
      if (!nzbd) {
        return;
      }
      setNoResults(false);
      setServices((current) => {
        const withoutOldValue = current.filter((item) => item.key !== nzbd.key);
        return [...withoutOldValue, nzbd].sort((a, b) => a.name.localeCompare(b.name));
      });
    });
    const lost = ServiceDiscovery.addEventListener('serviceLost', (service) => {
      if (active && isNzbdServiceType(service.type)) {
        const key = serviceKey(service);
        setServices((current) => current.filter((item) => item.key !== key));
      }
    });

    void start();
    return () => {
      active = false;
      found.remove();
      lost.remove();
      void ServiceDiscovery.stopSearch(SERVICE_TYPE).catch(() => undefined);
    };
  }, [start]);

  useEffect(() => {
    if (!scanning || services.length > 0 || error) return;
    const timeout = setTimeout(() => setNoResults(true), 8_000);
    return () => clearTimeout(timeout);
  }, [error, scanning, services.length]);

  return { services, scanning, noResults, error, scanAgain };
}

function discoveryError(cause: unknown): string {
  const detail = cause instanceof Error ? cause.message : String(cause);
  if (detail.includes("doesn't seem to be linked")) {
    return 'Nearby discovery needs a fresh native app build.';
  }
  return 'Could not scan the local network. Check Local Network permission and Wi-Fi.';
}
