package io.confluent.ksql;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;


public class DeSerStream {
  static class TestRecord {
    public long longField1;
    public long longField2;
    public String stringField;


  }

  public static class JSONDeserializer<T> implements Deserializer<T> {
    Class<T> tClass;
    ObjectMapper objectMapper;

    JSONDeserializer() {
      objectMapper = new ObjectMapper();
      objectMapper.registerModule(new AfterburnerModule());
    }

    @Override
    public void close() {
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
      try {
        return objectMapper.readValue(bytes, tClass);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> props, boolean key) {
      tClass = (Class<T>) props.get("JsonPOJOClass");

    }
  }

  public static class JSONSerializer<T> implements Serializer<T> {
    Class<T> tClass;
    ObjectMapper objectMapper;

    JSONSerializer() {
      objectMapper = new ObjectMapper();
      objectMapper.registerModule(new AfterburnerModule());
    }

    @Override
    public void close() {
    }

    @Override
    public byte[] serialize(String topic, T data) {
      try {
        return objectMapper.writeValueAsBytes(data);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> props, boolean key) {
      tClass = (Class<T>) props.get("JsonPOJOClass");

    }
  }

  static void go(String brokerAddress, String srcTopic, String dstTopic) {
    Properties cProps = new Properties();
    cProps.put("bootstrap.servers", brokerAddress);
    cProps.put("group.id", "consumer-test-tp");
    cProps.put("key.deserializer", StringDeserializer.class.getName());
    cProps.put("value.deserializer", JSONDeserializer.class.getName());
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cProps);

    Properties pProps = new Properties();
    pProps.put("boostrap.server", "localhost:9092");
    pProps.put("acks", "all");
    pProps.put("retries", 0);
    pProps.put("batch.size", 16384);
    pProps.put("linger.ms", 1);
    pProps.put("buffer.memory", 33554432);
    pProps.put("key.serializer", StringSerializer.class.getName());
    pProps.put("value.serializer", JSONSerializer.class.getName());
    KafkaProducer<String, String> producer = new KafkaProducer<>(pProps);

    consumer.subscribe(Arrays.asList(srcTopic));

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(1000);
    }
  }

  static int main(String[] args) {
    go(args[1], args[2], args[3]);
    return 0;
  }
}
