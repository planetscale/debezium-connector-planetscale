## Developing on the Connector

This is a standard Gradle codebase, using Kotlin. Build-time tooling targets any JDK at or above 21.

### Using Codespaces

This repository is equipped with a [GitHub Codespace][0] which is pre-configured for connector development. [Prebuilds][2] are configured for fast Codespace launching.

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/sgammon/debezium-connector-planetscale-v2?devcontainer_path=.devcontainer%2Fdevcontainer.json)

### Local Development

This codebase is a standard Gradle project, leveraging [Composite Builds][1]. The entire project can be treated as one thing, or you can scope
Gradle commands to the desired project.

To see the structure of projects, run:
```
./gradlew projects
```

### Upgrading Debezium

To upgrade the Debezium version (and, therefore, the connector versions), edit the version declared in the Debezium
version catalog, at `gradle/debezium.versions.toml`.

This version propagates everywhere:

- The upstream Debezium dependency version
- Resolution of unaligned dependencies
- The version of this library, which matches the upstream Debezium version

Any breakages resulting from an upgrade of Debezium should surface at compile-time, either during BuildBuddy's bytecode rewrites, or during
unit or integration testing.

### Publishing

Make sure you pass the right parameters to enable signing:
```
./gradlew \
  -Pplanetscale.release=true \
  -Pplanetscale.sigstore=true \
  build test check publish;
```

> [!TIP]
> If you just want to sign locally, set `planetscale.sigstore` to `false`. Neither of these properties are required to
> be set to run `publish`, which publishes to `./debezium-planetscale/build/m2`.

#### Uploading and Using the Artifacts

To publish these resources as a valid Maven repository via any S3-compliant service, navigate to this root, and use
`rclone` to copy the contents to the remote bucket:

```
cd debezium-planetscale/build/m2 && rclone --progress copy . [... remote bucket ...] && cd -
```

Then, use the remote bucket URL as the Maven repository URL in a downstream project. For example, in Gradle, and with a
remote bucket URL base of `https://maven.planetscale.com`:

**`settings.gradle.kts`**
```kotlin
dependencyResolutionManagement {
    repositories {
        mavenCentral()
        maven {
            name = "planetscale"
            url = uri("https://maven.planetscale.com/")
            content {
                includeGroup("com.planetscale.labs")
            }
        }
    }
}
```

Then, the connector dependency can be added with:
```kotlin
dependencies {
  // Version matches the upstream version of the Vitess connector.
  implementation("com.planetscale.labs:debezium-planetscale:3.2.1.Final")
}
```

#### Setting up Signing

To facilitate signing when publishing, follow Gradle's [directions for setting up GPG signing](https://docs.gradle.org/current/userguide/signing_plugin.html). Signing is required for publishing to Maven Central.

[0]: https://github.com/features/codespaces
[1]: https://docs.gradle.org/current/userguide/composite_builds.html
[2]: https://docs.github.com/en/codespaces/prebuilding-your-codespaces/about-github-codespaces-prebuilds
