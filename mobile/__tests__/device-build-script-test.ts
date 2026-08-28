describe('iOS device build script', () => {
  it('installs a self-contained Release build', () => {
    const packageJson = require('../package.json') as {
      scripts?: Record<string, string>;
    };

    expect(packageJson.scripts?.['ios:device']).toBe(
      'expo run:ios --configuration Release --device',
    );
  });
});
