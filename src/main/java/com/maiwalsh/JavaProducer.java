package com.maiwalsh;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class JavaProducer {
    public static void main(String[] args) {
        String topic = "test-topic"; // Set your Kafka topic

        // Kafka producer configuration
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "confluent-broker.recrocog.com:9092"); // Kafka broker address
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Kafka producer instance
        Producer<String, String> producer = new KafkaProducer<>(properties);

        // Send a simple message
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "key1", "Hello from the Producer!");
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                System.out.println("Sent message to topic: " + metadata.topic() + " partition: " + metadata.partition());
            } else {
                exception.printStackTrace();
            }
        });

        producer.close();
    }
}
