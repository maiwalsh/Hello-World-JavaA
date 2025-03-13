package com.maiwalsh;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;

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

        // Scanner for user input
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter messages to send to Kafka. Type 'exit' to stop.");

        while (true) {
            System.out.print("> ");
            String message = scanner.nextLine();

            if ("exit".equalsIgnoreCase(message)) {
                System.out.println("Exiting producer...");
                break;
            }

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, "key1", message);
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("Sent message: '" + message + "' to topic: " + metadata.topic() + 
                                       " | partition: " + metadata.partition() + 
                                       " | offset: " + metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            });
        }

        // Cleanup
        scanner.close();
        producer.close();
    }
}
