export interface NetworkService {
  readonly name: string;
  readonly type: string;
  readonly domain: string;
  readonly hostName: string;
  readonly addresses: string[];
  readonly port: number;
  readonly txt: Record<string, string>;
}

export interface DiscoveredNzbd {
  key: string;
  name: string;
  baseUrl: string;
  host: string;
  port: number;
  auth: string;
  version?: string;
}

export function isNzbdServiceType(type: string): boolean {
  const normalized = type.trim().toLowerCase().replace(/\.local\.$/, '.');
  return normalized === 'nzbd' || normalized === '_nzbd._tcp' || normalized === '_nzbd._tcp.';
}

export function serviceKey(service: Pick<NetworkService, 'name' | 'type' | 'domain'>): string {
  return `${service.name}|${service.type}|${service.domain}`;
}

export function toDiscoveredNzbd(service: NetworkService): DiscoveredNzbd | null {
  if (!Number.isInteger(service.port) || service.port < 1 || service.port > 65535) {
    return null;
  }

  const host = selectHost(service);
  if (!host) {
    return null;
  }
  const tls = ['1', 'true', 'yes'].includes((service.txt.tls ?? '').toLowerCase());
  const urlHost = host.includes(':') ? `[${host.replace('%', '%25')}]` : host;

  return {
    key: serviceKey(service),
    name: service.name || 'Runner',
    baseUrl: `${tls ? 'https' : 'http'}://${urlHost}:${service.port}`,
    host,
    port: service.port,
    auth: service.txt.auth || 'unknown',
    version: service.txt.version || undefined,
  };
}

function selectHost(service: NetworkService): string | null {
  const addresses = service.addresses.map(cleanHost).filter(Boolean) as string[];
  const ipv4 = addresses.find(
    (address) =>
      address.includes('.') &&
      !address.startsWith('127.') &&
      !address.startsWith('169.254.'),
  );
  if (ipv4) {
    return ipv4;
  }
  const ipv6 = addresses.find(
    (address) =>
      address.includes(':') &&
      address !== '::' &&
      address !== '::1' &&
      !address.toLowerCase().startsWith('fe80:'),
  );
  if (ipv6) {
    return ipv6;
  }
  const hostname = cleanHost(service.hostName);
  const fallbackAddress = addresses.find((address) => !address.startsWith('127.'));
  return hostname || fallbackAddress || null;
}

function cleanHost(host: string): string {
  return host.trim().replace(/^\[/, '').replace(/\]$/, '').replace(/\.$/, '');
}
