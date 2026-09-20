/** @jest-environment node */
/// <reference types="node" />

import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import plist from '@expo/plist';

const plugin = require('../plugins/withIosSceneLifecycle') as {
  mergeSceneManifest: (existing?: Record<string, unknown>) => Record<string, any>
  patchAppDelegate: (contents: string) => string
  sceneDelegateSource: string
}

const appDelegate = `import UIKit

class AppDelegate: ExpoAppDelegate {
  var reactNativeFactory: RCTReactNativeFactory?

  override func application(
    _ application: UIApplication,
    didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]? = nil
  ) -> Bool {
    reactNativeFactory = factory

    #if os(iOS) || os(tvOS)
    window = UIWindow(frame: UIScreen.main.bounds)
    factory.startReactNative(
      withModuleName: "main",
      in: window,
      launchOptions: launchOptions)
    #endif

    return super.application(application, didFinishLaunchingWithOptions: launchOptions)
  }
}
`

describe('withIosSceneLifecycle', () => {
  it('moves React Native startup out of AppDelegate', () => {
    const patched = plugin.patchAppDelegate(appDelegate)

    expect(patched).not.toContain('UIWindow(frame:')
    expect(patched).not.toContain('factory.startReactNative')
    expect(patched).toContain('reactNativeFactory = factory')
  })

  it('is idempotent', () => {
    const patched = plugin.patchAppDelegate(appDelegate)

    expect(plugin.patchAppDelegate(patched)).toBe(patched)
  })

  it('fails when the generated AppDelegate template drifts', () => {
    expect(() =>
      plugin.patchAppDelegate('factory.startReactNative(withModuleName: "main")'),
    ).toThrow(/AppDelegate template changed/)
  })

  it('ships the scene lifecycle in the committed native project and prebuild config', () => {
    const mobileRoot = join(__dirname, '..');
    const read = (path: string) => readFileSync(join(mobileRoot, path), 'utf8');
    const config = JSON.parse(read('app.json'));
    const info = plist.parse(read('ios/nzbd/Info.plist')) as Record<string, unknown>;
    const sceneDelegate = read('ios/nzbd/SceneDelegate.swift');

    expect(config.expo.plugins).toContain('./plugins/withIosSceneLifecycle');
    expect(info.UIApplicationSceneManifest).toEqual(plugin.mergeSceneManifest());
    expect(read('ios/nzbd/AppDelegate.swift')).not.toContain('factory.startReactNative');
    expect(read('ios/nzbd.xcodeproj/project.pbxproj')).toContain('SceneDelegate.swift in Sources');
    expect(sceneDelegate).toBe(plugin.sceneDelegateSource);
    expect(sceneDelegate).toContain(
      'let window = UIWindow(windowScene: windowScene)',
    )
    expect(sceneDelegate).toContain(
      'let factory = appDelegate.reactNativeFactory',
    )
    expect(sceneDelegate).toContain('withModuleName: "main"')
    expect(sceneDelegate).toContain(
      'launchOptions: launchOptions(from: connectionOptions)',
    )
    expect(sceneDelegate).toContain('openURLContexts URLContexts')
    expect(sceneDelegate).toContain('continue userActivity')
  })

  it('adds the application scene while preserving other scene roles', () => {
    const manifest = plugin.mergeSceneManifest({
      CustomKey: 'preserved',
      UISceneConfigurations: {
        UIWindowSceneSessionRoleExternalDisplay: [{ Name: 'external' }],
      },
    })

    expect(manifest.CustomKey).toBe('preserved')
    expect(manifest.UIApplicationSupportsMultipleScenes).toBe(false)
    expect(
      manifest.UISceneConfigurations.UIWindowSceneSessionRoleExternalDisplay,
    ).toEqual([{ Name: 'external' }])
    expect(
      manifest.UISceneConfigurations.UIWindowSceneSessionRoleApplication,
    ).toEqual([
      {
        UISceneConfigurationName: 'Default Configuration',
        UISceneDelegateClassName: '$(PRODUCT_MODULE_NAME).SceneDelegate',
      },
    ])
  })

  it('preserves a compatible existing application scene', () => {
    const applicationScenes = [
      {
        UISceneConfigurationName: 'Existing name',
        UISceneDelegateClassName: '$(PRODUCT_MODULE_NAME).SceneDelegate',
      },
    ]

    const manifest = plugin.mergeSceneManifest({
      UISceneConfigurations: {
        UIWindowSceneSessionRoleApplication: applicationScenes,
      },
    })

    expect(
      manifest.UISceneConfigurations.UIWindowSceneSessionRoleApplication,
    ).toEqual(applicationScenes)
  })

  it('rejects incompatible existing scene configuration', () => {
    expect(() =>
      plugin.mergeSceneManifest({
        UISceneConfigurations: {
          UIWindowSceneSessionRoleApplication: [
            { UISceneDelegateClassName: 'Other.SceneDelegate' },
          ],
        },
      }),
    ).toThrow(/incompatible application scene configuration/)

    expect(() =>
      plugin.mergeSceneManifest({ UIApplicationSupportsMultipleScenes: true }),
    ).toThrow(/multiple scenes are already enabled/)
  })
})
