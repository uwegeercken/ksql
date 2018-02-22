package io.confluent.ksql.physical;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Filter;
import org.rocksdb.Options;

import java.util.Map;


public class KsqlRocksDBConfigSetter implements RocksDBConfigSetter {
  public void setConfig(final String name, final Options options,
                        final Map<String, Object> config) {
    options.setAllowMmapReads(true);
    final BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
    tableConfig.setBlockCacheSize(0);
    tableConfig.setBlockSize(4096);
    final Filter bloomFilter = new BloomFilter(10);
    tableConfig.setFilter(bloomFilter);
    options.setTableFormatConfig(tableConfig);
  }
}
