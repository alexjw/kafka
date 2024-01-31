package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());
    public static void main(String[] args) {
        System.out.println("Hello World");
        log.info("Hello World");

        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("barch.size", "400");

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create a producer Record

        // Send Data -- async
        //for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 30; j++) {
                String topic = "demo_jar";
                String key = "id_" + j;
                String value = "Hello World " + j;
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
                producer.send(producerRecord, (recordMetadata, e) -> {
                    if(e == null) {
                        // Record was successfully sent
                        log.info("Received new Metadata\n" +
                                "Key: " + key + " | Partition: " + recordMetadata.partition() + " | Offset: " + recordMetadata.offset());
                    }
                    else {
                        log.error("Error while producing", e);
                    }
                });
            }
            /*try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }*/

        // Tell the producer to send all data and block until done -- synchronous
        producer.flush();

        // Flush and close the producer
        producer.close();
    }
}
