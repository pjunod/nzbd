const {
  IOSConfig,
  withAppDelegate,
  withInfoPlist,
} = require('expo/config-plugins')

const SCENE_DELEGATE_SOURCE = `import UIKit

class SceneDelegate: UIResponder, UIWindowSceneDelegate {
  var window: UIWindow?

  private var appDelegate: AppDelegate? {
    UIApplication.shared.delegate as? AppDelegate
  }

  func scene(
    _ scene: UIScene,
    willConnectTo session: UISceneSession,
    options connectionOptions: UIScene.ConnectionOptions
  ) {
    guard
      let windowScene = scene as? UIWindowScene,
      let appDelegate,
      let factory = appDelegate.reactNativeFactory
    else { return }

    let window = UIWindow(windowScene: windowScene)
    self.window = window
    appDelegate.window = window
    factory.startReactNative(
      withModuleName: "main",
      in: window,
      launchOptions: launchOptions(from: connectionOptions))
  }

  func scene(_ scene: UIScene, openURLContexts URLContexts: Set<UIOpenURLContext>) {
    guard let appDelegate else { return }

    for context in URLContexts {
      var options: [UIApplication.OpenURLOptionsKey: Any] = [
        .openInPlace: context.options.openInPlace,
      ]
      if let sourceApplication = context.options.sourceApplication {
        options[.sourceApplication] = sourceApplication
      }
      if let annotation = context.options.annotation {
        options[.annotation] = annotation
      }
      _ = appDelegate.application(
        UIApplication.shared,
        open: context.url,
        options: options)
    }
  }

  func scene(_ scene: UIScene, continue userActivity: NSUserActivity) {
    guard let appDelegate else { return }

    _ = appDelegate.application(
      UIApplication.shared,
      continue: userActivity,
      restorationHandler: { _ in })
  }

  private func launchOptions(
    from connectionOptions: UIScene.ConnectionOptions
  ) -> [UIApplication.LaunchOptionsKey: Any] {
    var launchOptions: [UIApplication.LaunchOptionsKey: Any] = [:]

    if let context = connectionOptions.urlContexts.first {
      launchOptions[.url] = context.url
      if let sourceApplication = context.options.sourceApplication {
        launchOptions[.sourceApplication] = sourceApplication
      }
      if let annotation = context.options.annotation {
        launchOptions[.annotation] = annotation
      }
    }

    if let userActivity = connectionOptions.userActivities.first {
      launchOptions[.userActivityDictionary] = [
        UIApplication.LaunchOptionsKey.userActivityType.rawValue:
          userActivity.activityType,
        "UIApplicationLaunchOptionsUserActivityKey": userActivity,
      ]
    }

    return launchOptions
  }
}
`

const SCENE_DELEGATE_CLASS = '$(PRODUCT_MODULE_NAME).SceneDelegate'

const LEGACY_STARTUP_PATTERN = /\n\s*#if os\(iOS\) \|\| os\(tvOS\)\n\s*window = UIWindow\(frame: UIScreen\.main\.bounds\)\n\s*factory\.startReactNative\(\n\s*withModuleName: "main",\n\s*in: window,\n\s*launchOptions: launchOptions\)\n\s*#endif\n/

function patchAppDelegate(contents) {
  if (!contents.includes('factory.startReactNative')) {
    return contents
  }

  if (!LEGACY_STARTUP_PATTERN.test(contents)) {
    throw new Error(
      'Unable to move React Native startup into SceneDelegate: AppDelegate template changed.',
    )
  }

  return contents.replace(LEGACY_STARTUP_PATTERN, '\n')
}

function mergeSceneManifest(existing) {
  const manifest = existing ?? {}
  if (manifest.UIApplicationSupportsMultipleScenes === true) {
    throw new Error(
      'Unable to configure SceneDelegate: multiple scenes are already enabled.',
    )
  }

  const sceneConfigurations = manifest.UISceneConfigurations ?? {}
  const applicationConfigurations =
    sceneConfigurations.UIWindowSceneSessionRoleApplication

  if (applicationConfigurations !== undefined) {
    const isCompatible =
      Array.isArray(applicationConfigurations) &&
      applicationConfigurations.length === 1 &&
      applicationConfigurations[0]?.UISceneDelegateClassName ===
        SCENE_DELEGATE_CLASS

    if (!isCompatible) {
      throw new Error(
        'Unable to configure SceneDelegate: an incompatible application scene configuration already exists.',
      )
    }
  }

  return {
    ...manifest,
    UIApplicationSupportsMultipleScenes: false,
    UISceneConfigurations: {
      ...sceneConfigurations,
      UIWindowSceneSessionRoleApplication: applicationConfigurations ?? [
        {
          UISceneConfigurationName: 'Default Configuration',
          UISceneDelegateClassName: SCENE_DELEGATE_CLASS,
        },
      ],
    },
  }
}

function withIosSceneLifecycle(config) {
  config = withAppDelegate(config, (modConfig) => {
    modConfig.modResults.contents = patchAppDelegate(
      modConfig.modResults.contents,
    )
    return modConfig
  })

  config = withInfoPlist(config, (modConfig) => {
    modConfig.modResults.UIApplicationSceneManifest = mergeSceneManifest(
      modConfig.modResults.UIApplicationSceneManifest,
    )
    return modConfig
  })

  return IOSConfig.XcodeProjectFile.withBuildSourceFile(config, {
    filePath: 'SceneDelegate.swift',
    contents: SCENE_DELEGATE_SOURCE,
    overwrite: true,
  })
}

module.exports = withIosSceneLifecycle
module.exports.mergeSceneManifest = mergeSceneManifest
module.exports.patchAppDelegate = patchAppDelegate
module.exports.sceneDelegateSource = SCENE_DELEGATE_SOURCE
