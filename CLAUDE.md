# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Test Commands

### Basic Development Tasks
- `./gradlew build` - Build all projects (includes bytecode transformation, shadowing, and packaging)
- `./gradlew test` - Run all tests across all projects
- `./gradlew check` - Run all checks including tests, detekt linting, and API validation
- `./gradlew clean` - Clean all build artifacts
- `./gradlew assemble` - Build JAR files without running tests

### Specific Project Tasks
- `./gradlew :debezium-planetscale:test` - Run only main connector tests
- `./gradlew :transforms:test` - Run only bytecode transformation tests
- `./gradlew :transformer:test` - Run only transformer plugin tests

### Code Quality
- `./gradlew detekt` - Run Kotlin linting
- `./gradlew spotlessCheck` - Check code formatting
- `./gradlew spotlessApply` - Apply code formatting

### Release Tasks
- `./gradlew publish` - Publish to local Maven repository (build/m2)
- `./gradlew -Pplanetscale.release=true -Pplanetscale.sigstore=true build test check publish` - Full release build with signing

## Architecture Overview

This project creates a Planetscale-specific Debezium connector by adapting the upstream Debezium Vitess connector through **bytecode transformation** rather than source code forking. This approach enables rapid adoption of upstream updates while maintaining custom Planetscale-specific functionality.

### Key Components

1. **debezium-planetscale/** - Main connector project that produces the final shadowed JAR
2. **transforms/** - ByteBuddy transformation plugins that modify upstream Vitess connector classes at build time
3. **transformer/** - Gradle plugin for codegen and build-time transformations
4. **build-logic/** - Shared Gradle build conventions and version catalogs

### Bytecode Transformation Process

The build process:
1. Downloads upstream Debezium Vitess connector from Maven
2. Unpacks and copies upstream classes to `build/debezium/classes`
3. Applies ByteBuddy transformations via plugins in `transforms/src/main/kotlin/com/planetscale/codegen/transforms/`
4. Creates partially-shadowed JAR with:
   - Transformed upstream classes relocated to `com.planetscale.labs.io.debezium.connector.*`
   - Local Planetscale-specific classes at `com.planetscale.debezium.*`
   - Rewritten service files for Kafka Connect SPI

### Key Transformation Classes

- `VitessManagedChannel.kt` - Intercepts gRPC channel creation for Planetscale authentication
- `VitessGeometry.kt` - Adds spatial data type support (GEOMETRY columns)  
- `VitessValueResolver.kt` - Custom value resolution for Planetscale-specific data handling

## Development Workflow

### When Making Changes to Transformations

1. Edit transformation classes in `transforms/src/main/kotlin/com/planetscale/codegen/transforms/`
2. Corresponding handler classes go in `transforms/src/main/kotlin/com/planetscale/debezium/`
3. Run `./gradlew :transforms:test` to verify transformation logic
4. Run `./gradlew :debezium-planetscale:build` to apply transformations and create connector JAR
5. Test integration with `./gradlew :debezium-planetscale:test`

### When Upgrading Debezium

Edit `gradle/debezium.versions.toml` to update the upstream Debezium version. This automatically propagates to:
- Dependency resolution
- Library version matching
- Transformation compatibility checks

### Testing Strategy

- Unit tests in each module test specific functionality
- `ConnectorStartTest.kt` - Validates connector initialization
- `VitessIntegrationTest.kt` - End-to-end connector testing
- Geometry tests validate spatial data handling
- Channel tests verify gRPC authentication

## Important Files and Locations

- `gradle/libs.versions.toml` - Main dependency version catalog
- `gradle/debezium.versions.toml` - Debezium-specific versions
- `config/detekt.yml` - Kotlin linting configuration  
- `test-connector-config.json` - Sample connector configuration for testing
- `test-geometry-setup.sql` - SQL for geometry feature testing

## Publishing and Distribution

The connector produces:
- **JAR**: `planetscale-debezium-adapter-${version}.jar` in `debezium-planetscale/build/libs/`
- **ZIP**: Kafka Connect distribution in `debezium-planetscale/build/connect/dist/`
- **Maven**: Local repository in `debezium-planetscale/build/m2/`

The final JAR can be used as a drop-in replacement for the upstream Vitess connector with identical dependencies but Planetscale-specific enhancements.