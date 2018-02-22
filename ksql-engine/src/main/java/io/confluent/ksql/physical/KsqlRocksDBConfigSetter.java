package io.confluent.ksql.physical;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.Options;

import java.util.Map;


public class KsqlRocksDBConfigSetter implements RocksDBConfigSetter {
  public void setConfig(final String name, final Options options,
                        final Map<String, Object> config) {
    options.setAllowMmapReads(true);
  }
}
