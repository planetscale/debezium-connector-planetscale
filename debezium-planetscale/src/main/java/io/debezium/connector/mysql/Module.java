package io.debezium.connector.mysql;

import io.debezium.util.IoUtil;
import java.util.Properties;

public class Module {
  private static final Properties INFO = IoUtil.loadProperties(Module.class, "com/planetscale/labs/io/debezium/connector/mysql/build.version");

  public Module() {
  }

  public static String version() {
    return INFO.getProperty("version");
  }

  public static String name() {
    return "mysql";
  }

  public static String contextName() {
    return "MySQL";
  }
}
