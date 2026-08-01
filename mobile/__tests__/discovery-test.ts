import {
  isNzbdServiceType,
  serviceKey,
  toDiscoveredNzbd,
} from '../src/discovery/nzbdService';

const service = {
  name: 'nzbd on studio',
  type: '_nzbd._tcp.',
  domain: 'local.',
  hostName: 'studio.local.',
  addresses: ['fe80::1', '169.254.9.219', '192.168.1.42'],
  port: 6789,
  txt: { path: '/api/v1', tls: '0', auth: 'bearer', version: '0.2.0' },
};

test('turns an nzbd service into a reachable connection', () => {
  expect(toDiscoveredNzbd(service)).toEqual({
    key: 'nzbd on studio|_nzbd._tcp.|local.',
    name: 'nzbd on studio',
    baseUrl: 'http://192.168.1.42:6789',
    host: '192.168.1.42',
    port: 6789,
    auth: 'bearer',
    version: '0.2.0',
  });
});

test('uses TLS metadata and brackets IPv6 addresses', () => {
  expect(
    toDiscoveredNzbd({
      ...service,
      addresses: ['2001:db8::8'],
      txt: { tls: '1', auth: 'none' },
    })?.baseUrl,
  ).toBe('https://[2001:db8::8]:6789');
});

test('falls back to the advertised hostname and rejects invalid ports', () => {
  expect(toDiscoveredNzbd({ ...service, addresses: [] })?.host).toBe('studio.local');
  expect(toDiscoveredNzbd({ ...service, addresses: ['127.0.0.1', '::1'] })?.host).toBe(
    'studio.local',
  );
  expect(toDiscoveredNzbd({ ...service, port: 0 })).toBeNull();
  expect(serviceKey(service)).toBe('nzbd on studio|_nzbd._tcp.|local.');
});

test('accepts native DNS-SD type variants for nzbd', () => {
  expect(isNzbdServiceType('_nzbd._tcp.')).toBe(true);
  expect(isNzbdServiceType('_NZBD._tcp.local.')).toBe(true);
  expect(isNzbdServiceType('nzbd')).toBe(true);
  expect(isNzbdServiceType('_http._tcp.')).toBe(false);
});
