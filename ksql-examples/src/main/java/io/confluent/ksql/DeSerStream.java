/**
 * Copyright 2017 Confluent Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql;

import java.util.*;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;


public class DeSerStream {
  public static class TestRecord {
    public long long_field_1;
    public long long_field_2;
    public String string_field;
  }

  public static class TestRecordDeserializer implements Deserializer<TestRecord> {
    public ObjectMapper objectMapper;

    public TestRecordDeserializer() {
      objectMapper = new ObjectMapper();
      objectMapper.registerModule(new AfterburnerModule());
      objectMapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE);
      objectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
    }

    @Override
    public void close() {}

    @Override
    public TestRecord deserialize(String topic, byte[] bytes) {
      try {
        return objectMapper.readValue(bytes, TestRecord.class);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> props, boolean key) {}
  }

  public static class TestRecordSerializer implements Serializer<TestRecord> {
    public ObjectMapper objectMapper;

    public TestRecordSerializer() {
      objectMapper = new ObjectMapper();
      objectMapper.registerModule(new AfterburnerModule());
    }

    @Override
    public void close() {}

    @Override
    public byte[] serialize(String topic, TestRecord data) {
      try {
        return objectMapper.writeValueAsBytes(data);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> props, boolean key) {}
  }

  static boolean keepRunning = true;

  static void go(List<Long> counts, int i, String brokerAddress, String srcTopic,
                 String dstTopic, int bufferSz, int bufferMemory) {
    Properties cProps = new Properties();
    cProps.put("bootstrap.servers", brokerAddress);
    Random rnd = new Random();
    cProps.put("group.id", "consumer-test-tp-" + rnd.nextInt());
    cProps.put("key.deserializer", StringDeserializer.class.getName());
    cProps.put("value.deserializer", TestRecordDeserializer.class.getName());
    cProps.put("auto.offset.reset", "earliest");
    KafkaConsumer<String, TestRecord> consumer = new KafkaConsumer<>(cProps);

    Properties pProps = new Properties();
    pProps.put("bootstrap.servers", brokerAddress);
    pProps.put("acks", "all");
    pProps.put("retries", 10);
    pProps.put("batch.size", bufferSz);
    pProps.put("linger.ms", 100);
    pProps.put("buffer.memory", bufferMemory);
    pProps.put("key.serializer", StringSerializer.class.getName());
    pProps.put("value.serializer", TestRecordSerializer.class.getName());
    KafkaProducer<String, TestRecord> producer = new KafkaProducer<>(pProps);

    consumer.subscribe(Arrays.asList(srcTopic));

    System.out.println(String.format("Go src(%s) dst(%s) bufferSz(%d) bufferMem(%d)", srcTopic, dstTopic, bufferSz, bufferMemory));

    while (keepRunning) {
      ConsumerRecords<String, TestRecord> records = consumer.poll(1000);
      for (ConsumerRecord<String, TestRecord> r : records) {
        producer.send(new ProducerRecord<>(dstTopic, Long.toString(r.value().long_field_1), r.value()));
        counts.set(i, counts.get(i) + 1);
      }
    }

    consumer.close();
    producer.close();
  }

  public static void main(String[] args) {
    List<Thread> threads = new LinkedList<>();
    ArrayList<Long> counts = new ArrayList<>();
    for (int i = 0; i < Integer.valueOf(args[0]); i++) {
      counts.add((long)0);
      final int c = i;
      Thread t = new Thread(() -> {
          go(counts, c, args[1], args[2], args[3],
              Integer.valueOf(args[4]), Integer.valueOf(args[5]));
        }
      );
      // t.setDaemon(true);
      t.start();
      threads.add(t);
    }

    long total = 0;

    Thread stopThread = new Thread() {
      public void run() {
        keepRunning = false;
        for (Thread t : threads) {
          try {
            t.join();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        int total = 0;
        for (int i = 0; i < threads.size(); i++) {
          total += counts.get(i);
        }
        System.out.println("Total: " + Integer.toString(total));
      }
    };
    Runtime.getRuntime().addShutdownHook(stopThread);
  }
}
