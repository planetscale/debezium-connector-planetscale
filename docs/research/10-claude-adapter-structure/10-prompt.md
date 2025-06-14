I am designing a project which, in essence, "forks" the Vitess adapter, but in a creative way. I would like your help.

We are writing a Debezium adapter for Planetscale, by forking the Vitess adapter, but in a way that does not require custom code changes.

We will assemble a project by pulling in code from Maven repositories upstream, with the Vitess adapter, and then patch bytecode into methods which we override. We will use ByteBuddy and Gradle to do this. Thus, we will be able to ship our own shaded JAR PlanetScale adapter, which, under the hood, uses the Vitess adapter, but without forking it.

Here is what I want your help doing, to start:

- Identify the upstream Maven repositories (snapshots? custom?) where we can get Debezium's base dependencies from, both for the core Debezium software and also the Vitess adapter.
- I already have a project created in Gradle. It is configured with defaults, and has support for Kotlin. We should use prefer Kotlin.
- We need to wire together ByteBuddy support at build time, and also shading/fat JAR support.
- Our shading support will need to shade Vitess adapter classes to a private path.
- We will need to apply bytecode transformations at build time.
- The bytecode transformations will delegate certain behaviors to our own hooks, implemented in Kotlin.
- We should situate the ByteBuddy "wiring" logic separately from the actual implementations, to facilitate an eventual migration of this code, upstream, into the mainline Vitess adapter.

Let's get started there. Please go research and tell me how we can best approach all of this, specifically looking to answer all of the points above.
