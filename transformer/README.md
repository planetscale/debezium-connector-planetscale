## Planetscale Transformer

This module implements the transformer plugin infrastructure for use of the Vitess Debezium adapter with Planetscale;
the [`transforms`](../transforms) module defines code which overrides various methods in the upstream adapter with
custom implementation code.

Those are applied here, and made available on the build-time classpath, through the combination of a [ByteBuddy][0]
plugin and Gradle plugin.

> [!NOTE]
> The transforms themselves are **here**; the [`transformer`](../transformer) module is where these are applied.

[0]: https://bytebuddy.net/
