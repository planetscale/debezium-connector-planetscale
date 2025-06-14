package com.planetscale.debezium.hello

object DebeziumVitessHello {
  @JvmStatic fun start(props: java.util.Map<String, String>?) {
    println("Would start Vitess connector: $props")
  }
}
