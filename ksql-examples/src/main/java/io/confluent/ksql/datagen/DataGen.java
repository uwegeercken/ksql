/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.datagen;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.List;
import java.util.LinkedList;

import io.confluent.avro.random.generator.Generator;
import io.confluent.ksql.util.KsqlConfig;

public class DataGen {

  public static void main(String[] args) {
    Arguments arguments;

    try {
      arguments = new Arguments.Builder().parseArgs(args).build();
    } catch (Arguments.ArgumentParseException exception) {
      System.err.println(exception.getMessage());
      usage(1);
      return;
    } catch (IOException exception) {
      System.err.printf("IOException encountered: %s%n", exception.getMessage());
      return;
    }

    if (arguments.help) {
      usage(0);
    }

    DataGenProducer dataProducer;

    switch (arguments.format) {
      case AVRO:
        dataProducer = new AvroProducer(
            new KsqlConfig(Collections.singletonMap(
                KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY,
                arguments.schemaRegistryUrl
            )));
        break;
      case JSON:
        dataProducer = new JsonProducer();
        break;
      case DELIMITED:
        dataProducer = new DelimitedProducer();
        break;
      default:
        System.err.printf(
            "Invalid format in '%s'; was expecting one of AVRO, JSON, or DELIMITED%n",
            arguments.format
        );
        usage(1);
        return;
    }

    long start = System.currentTimeMillis();
    String schema = "";
    while (true) {
      byte[] b = new byte[256];
      int ret;
      try {
        ret = arguments.schemaFile.read(b);
      } catch (IOException exception) {
        System.err.printf("IOException encountered: %s%n", exception.getMessage());
        return;
      }
      if (ret < 0) {
        break;
      }
      schema += new String(b);
    }

    final TokenBucket tokenBucket;
    if (arguments.msgRate != -1) {
      tokenBucket = new TokenBucket(arguments.msgRate, arguments.msgRate);
    } else {
      tokenBucket = null;
    }

    List<Thread> threads = new LinkedList<>();
    for (int i = 0; i < arguments.numThreads; i++) {
      Properties props = arguments.properties;
      props.put("bootstrap.servers", arguments.bootstrapServer);
      props.put("client.id", "KSQLDataGenProducer");
      System.out.println("Producer Properties:" + props);

      try {
        if (arguments.propertiesFile != null) {
          props.load(arguments.propertiesFile);
        }
      } catch (IOException exception) {
        System.err.printf("IOException encountered: %s%n", exception.getMessage());
        return;
      }

      final TimestampGenerator timestampGenerator;
      if (arguments.timeIncrement > 0) {
        timestampGenerator = new TimestampGenerator(
            start, arguments.timeIncrement, arguments.timeBurst);
      } else {
        timestampGenerator = null;
      }

      final Generator generator;
      generator = new Generator(schema, new Random());

      Thread t = new Thread(() -> {
        try {
          dataProducer.populateTopic(props, generator, arguments.topicName, arguments.keyName,
              arguments.iterations, arguments.maxInterval, arguments.printRows, timestampGenerator,
              tokenBucket);
        } catch (InterruptedException e) {
          System.err.println("Producer thread interrupted");
        }
      });
      t.setDaemon(true);
      t.start();
      threads.add(t);
    }

    for (Thread t : threads) {
      try {
        t.join();
      } catch (InterruptedException e) {
        // interrupted. exit with error
        System.exit(1);
      }
    }
  }

  private static void usage() {
    System.err.println(
        "usage: DataGen "
        + "[help] "
        + "[bootstrap-server=<kafka bootstrap server(s)> (defaults to localhost:9092)] "
        + "[quickstart=<quickstart preset> (case-insensitive; one of 'orders', 'users', or "
        + "'pageviews')] "
        + "schema=<avro schema file> "
        + "format=<message format> (case-insensitive; one of 'avro', 'json', or 'delimited') "
        + "topic=<kafka topic name> "
        + "key=<name of key column> "
        + "[iterations=<number of rows> (defaults to 1,000,000)] "
        + "[maxInterval=<Max time in ms between rows> (defaults to 500)] "
        + "[propertiesFile=<file specifying Kafka client properties>]"
    );
  }

  private static void usage(int exitValue) {
    usage();
    System.exit(exitValue);
  }

  private static class Arguments {

    public enum Format {AVRO, JSON, DELIMITED}

    public final boolean help;
    public final String bootstrapServer;
    public final InputStream schemaFile;
    public final Format format;
    public final String topicName;
    public final String keyName;
    public final int iterations;
    public final long maxInterval;
    public final String schemaRegistryUrl;
    public final boolean printRows;
    public final Properties properties;
    public final int numThreads;
    public final InputStream propertiesFile;
    public final long timeIncrement;
    public final long timeBurst;
    public final int msgRate;

    public Arguments(
        boolean help,
        String bootstrapServer,
        InputStream schemaFile,
        Format format,
        String topicName,
        String keyName,
        int iterations,
        long maxInterval,
        String schemaRegistryUrl,
        boolean printRows,
        Properties properties,
        int numThreads,
        InputStream propertiesFile,
        long timeIncrement,
        long timeBurst,
        int msgRate
    ) {
      this.help = help;
      this.bootstrapServer = bootstrapServer;
      this.schemaFile = schemaFile;
      this.format = format;
      this.topicName = topicName;
      this.keyName = keyName;
      this.iterations = iterations;
      this.maxInterval = maxInterval;
      this.schemaRegistryUrl = schemaRegistryUrl;
      this.printRows = printRows;
      this.properties = properties;
      this.numThreads = numThreads;
      this.propertiesFile = propertiesFile;
      this.timeIncrement = timeIncrement;
      this.timeBurst = timeBurst;
      this.msgRate = msgRate;
    }

    public static class ArgumentParseException extends RuntimeException {

      public ArgumentParseException(String message) {
        super(message);
      }
    }

    public static class Builder {

      private Quickstart quickstart;

      private boolean help;
      private String bootstrapServer;
      private InputStream schemaFile;
      private Format format;
      private String topicName;
      private String keyName;
      private int iterations;
      private long maxInterval;
      private String schemaRegistryUrl;
      private boolean printRows;
      private Properties properties;
      private int numThreads;
      private InputStream propertiesFile;
      private long timeIncrement;
      private long timeBurst;
      private int msgRate;

      public Builder() {
        quickstart = null;
        help = false;
        bootstrapServer = "localhost:9092";
        schemaFile = null;
        format = null;
        topicName = null;
        keyName = null;
        iterations = 1000000;
        maxInterval = -1;
        schemaRegistryUrl = "http://localhost:8081";
        printRows = true;
        properties = new Properties();
        numThreads = 1;
        propertiesFile = null;
        timeIncrement = -1;
        timeBurst = 1;
        msgRate = -1;
      }

      private enum Quickstart {
        CLICKSTREAM_CODES("clickstream_codes_schema.avro", "clickstream", "code"),
        CLICKSTREAM("clickstream_schema.avro", "clickstream", "ip"),
        CLICKSTREAM_USERS("clickstream_users_schema.avro", "webusers", "user_id"),
        ORDERS("orders_schema.avro", "orders", "orderid"),
        RATINGS("ratings_schema.avro", "ratings", "rating_id"),
        USERS("users_schema.avro", "users", "userid"),
        USERS_("users_array_map_schema.avro", "users", "userid"),
        PAGEVIEWS("pageviews_schema.avro", "pageviews", "viewtime");

        private final String schemaFileName;
        private final String rootTopicName;
        private final String keyName;

        Quickstart(String schemaFileName, String rootTopicName, String keyName) {
          this.schemaFileName = schemaFileName;
          this.rootTopicName = rootTopicName;
          this.keyName = keyName;
        }

        public InputStream getSchemaFile() {
          return getClass().getClassLoader().getResourceAsStream(schemaFileName);
        }

        public String getTopicName(Format format) {
          return String.format("%s_kafka_topic_%s", rootTopicName, format.name().toLowerCase());
        }

        public String getKeyName() {
          return keyName;
        }

        public Format getFormat() {
          return Format.JSON;
        }

      }

      public Arguments build() {
        if (help) {
          return new Arguments(true, null, null, null, null,
              null, 0, -1, null, true, null,
              1, null, -1, -1, -1);
        }

        if (quickstart != null) {
          schemaFile = Optional.ofNullable(schemaFile).orElse(quickstart.getSchemaFile());
          format = Optional.ofNullable(format).orElse(quickstart.getFormat());
          topicName = Optional.ofNullable(topicName).orElse(quickstart.getTopicName(format));
          keyName = Optional.ofNullable(keyName).orElse(quickstart.getKeyName());
        }

        try {
          Objects.requireNonNull(schemaFile, "Schema file not provided");
          Objects.requireNonNull(format, "Message format not provided");
          Objects.requireNonNull(topicName, "Kafka topic name not provided");
          Objects.requireNonNull(keyName, "Name of key column not provided");
        } catch (NullPointerException exception) {
          throw new ArgumentParseException(exception.getMessage());
        }
        return new Arguments(
            help,
            bootstrapServer,
            schemaFile,
            format,
            topicName,
            keyName,
            iterations,
            maxInterval,
            schemaRegistryUrl,
            printRows,
            properties,
            numThreads,
            propertiesFile,
            timeIncrement,
            timeBurst,
            msgRate
        );
      }

      public Builder parseArgs(String[] args) throws IOException {
        for (String arg : args) {
          parseArg(arg);
        }
        return this;
      }

      public Builder parseArg(String arg) throws IOException {

        if ("help".equals(arg)) {
          help = true;
          return this;
        }

        String[] splitOnEquals = arg.split("=");
        if (splitOnEquals.length != 2) {
          throw new ArgumentParseException(String.format(
              "Invalid argument format in '%s'; expected <name>=<value>",
              arg
          ));
        }

        String argName = splitOnEquals[0].trim();
        String argValue = splitOnEquals[1].trim();

        if (argName.isEmpty()) {
          throw new ArgumentParseException(String.format(
              "Empty argument name in %s",
              arg
          ));
        }

        if (argValue.isEmpty()) {
          throw new ArgumentParseException(String.format(
              "Empty argument value in '%s'",
              arg
          ));
        }

        if (argName.startsWith("producer")) {
          String property = argName.substring("producer".length() + 1);
          Object value = parsePropertyValue(argValue);
          properties.put(property, value);
          return this;
        }

        switch (argName) {
          case "quickstart":
            try {
              quickstart = Quickstart.valueOf(argValue.toUpperCase());
            } catch (IllegalArgumentException iae) {
              throw new ArgumentParseException(String.format(
                  "Invalid quickstart in '%s'; was expecting one of "
                  + Arrays.toString(Quickstart.values())
                  + " (case-insensitive)",
                  argValue
              ));
            }
            break;
          case "bootstrap-server":
            bootstrapServer = argValue;
            break;
          case "schema":
            schemaFile = new FileInputStream(argValue);
            break;
          case "format":
            format = parseFormat(argValue);
            break;
          case "topic":
            topicName = argValue;
            break;
          case "key":
            keyName = argValue;
            break;
          case "iterations":
            iterations = parseIterations(argValue);
            break;
          case "maxInterval":
            maxInterval = parseMaxInterval(argValue);
            break;
          case "schemaRegistryUrl":
            schemaRegistryUrl = argValue;
            break;
          case "printRows":
            printRows = parsePrintRows(argValue);
            break;
          case "numThreads":
            numThreads = parseNThreads(argValue);
            break;
          case "propertiesFile":
            propertiesFile = new FileInputStream(argValue);
            break;
          case "timeIncrement":
            timeIncrement = parseTimeIncrement(argValue);
            break;
          case "timeBurst":
            timeBurst = parseTimeBurst(argValue);
            break;
          case "msgRate":
            msgRate = parseMsgRate(argValue);
            break;
          default:
            throw new ArgumentParseException(String.format(
                "Unknown argument name in '%s'",
                argName
            ));
        }
        return this;
      }

      private Format parseFormat(String formatString) {
        try {
          return Format.valueOf(formatString.toUpperCase());
        } catch (IllegalArgumentException exception) {
          throw new ArgumentParseException(String.format(
              "Invalid format in '%s'; was expecting one of AVRO, JSON, or DELIMITED "
              + "(case-insensitive)",
              formatString
          ));
        }
      }

      private int parseIterations(String iterationsString) {
        try {
          int result = Integer.valueOf(iterationsString, 10);
          if (result <= 0) {
            throw new ArgumentParseException(String.format(
                "Invalid number of iterations in '%d'; must be a positive number",
                result
            ));
          }
          return Integer.valueOf(iterationsString, 10);
        } catch (NumberFormatException exception) {
          throw new ArgumentParseException(String.format(
              "Invalid number of iterations in '%s'; must be a valid base 10 integer",
              iterationsString
          ));
        }
      }

      private int parseNThreads(String numThreadsString) {
        try {
          int result = Integer.valueOf(numThreadsString, 10);
          if (result < 0) {
            throw new ArgumentParseException(String.format(
                "Invalid number of threads in '%d'; must be a positive number",
                result));
          }
          return result;
        } catch (NumberFormatException e) {
          throw new ArgumentParseException(String.format(
              "Invalid number of threads in '%s'; must be a positive number",
              numThreadsString));
        }
      }

      private int parseTimeIncrement(String timeIncrementString) {
        try {
          int result = Integer.valueOf(timeIncrementString, 10);
          if (result < 0) {
            throw new ArgumentParseException(String.format(
                "Invalid time increment in '%d'; must be a positive number",
                result));
          }
          return result;
        } catch (NumberFormatException e) {
          throw new ArgumentParseException(String.format(
              "Invalid time increment in '%s'; must be a positive number",
              timeIncrementString));
        }
      }

      private int parseTimeBurst(String timeBurstString) {
        try {
          int result = Integer.valueOf(timeBurstString, 10);
          if (result < 0) {
            throw new ArgumentParseException(String.format(
                "Invalid time burst in '%d'; must be a positive number",
                result));
          }
          return result;
        } catch (NumberFormatException e) {
          throw new ArgumentParseException(String.format(
              "Invalid time burst in '%s'; must be a positive number",
              timeBurstString));
        }
      }

      private int parseMsgRate(String msgRateString) {
        try {
          int result = Integer.valueOf(msgRateString, 10);
          if (result < 0) {
            throw new ArgumentParseException(String.format(
                "Invalid msg rate in '%d'; must be a positive number",
                result));
          }
          return result;
        } catch (NumberFormatException e) {
          throw new ArgumentParseException(String.format(
              "Invalid msg rate in '%s'; must be a positive number",
              msgRateString));
        }
      }

      private long parseMaxInterval(String maxIntervalString) {
        try {
          long result = Long.valueOf(maxIntervalString, 10);
          if (result < 0) {
            throw new ArgumentParseException(String.format(
                "Invalid number of maxInterval in '%d'; must be a positive number",
                result
            ));
          }
          return Long.valueOf(maxIntervalString, 10);
        } catch (NumberFormatException exception) {
          throw new ArgumentParseException(String.format(
              "Invalid number of maxInterval in '%s'; must be a valid base 10 long",
              maxIntervalString
          ));
        }
      }

      private boolean parsePrintRows(String printRowsString) {
        switch(printRowsString.toLowerCase()) {
          case "false":
            return false;
          case "true":
            return true;
          default:
            throw new ArgumentParseException(String.format(
                "Invalid value for printRows in '%s'; must be true or false",
                printRowsString
            ));
        }
      }

      private Object parsePropertyValue(String propertyValueString) {
        String[] split = propertyValueString.split(",", 2);
        if (split.length != 2) {
          throw new ArgumentParseException("Invalid value for property in %s; must be in format <type>,<value>");
        }
        switch(split[0]) {
          case "int":
            return Integer.valueOf(split[1]);
          case "string":
            return split[1];
          default:
            throw new ArgumentParseException("Invalid type for property in %s; must be string or int");
        }
      }
    }
  }
}
