package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    public static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
        System.out.println("Hello World");
        log.info("Hello World");

        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create a producer Record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_jar", "Hello world");

        // Send Data -- async
        producer.send(producerRecord);

        // Tell the producer to send all data and block until done -- synchronous
        producer.flush();

        // Flush and close the producer
        producer.close();
    }
}
