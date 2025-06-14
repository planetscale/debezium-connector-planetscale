## Planetscale Transforms

This module implements the actual transforms to upgrade the Vitess Debezium adapter for use with Planetscale. In
addition to the implementations themselves, [ByteBuddy][0] plugins are defined which apply the implementations to the
adapter, by transforming or generating bytecode at build-time.

> [!NOTE]
> The transforms themselves are **here**; the [`transformer`](../transformer) module is where these are applied.

[0]: https://bytebuddy.net/
