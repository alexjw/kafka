package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import com.launchdarkly.eventsource.EventHandler;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";

        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // This is kafka 3.3, defaults
        // since kafka 3.0 producer is "safe" by default
        // default configs:
        // acks=all(-1) this means that the leader and all the replicas wrote the data successfuly,
        //              ack=0 doesn't care, and ack=1 cares only about the leader
        // min.insync.replicas=2 this means that if we have 1 replica, it must ack the leader and 1 replica,
        //                       no possibility of a replica going down, if we have 2 replicas, 1 replica can go down,
        //                       it's not good to have ack ALL replica, there must be some room in case of failure
        // retries=2147483647   number of times a producer will retry to write the topic
        // delivery.timeout.ms=120000   number of retries is limited by this timeout
        // max.in.flight.requests=5  this keeps order after kafka 1.0
        // enable.idempotence=true  this will prevent duplicates in case the server fails to send the ack and the
        //                          producer sends the data again

        // Compression
        // the bigger the batch, better compression
        // snappy/sz4 produces better speed/compression ratio
        // better to have compression in producer instead of the broker
        // defaults
        // compression.type = none default no compression, compression is recommended for prod environments
        // batch.size=16kb  Increasing could help compression at the cost of latency
        //                  any message bigger than the batch will not be batched and sent immediately
        // linger.ms = 0    the producer will wait this time fot the batches to fill up before sending them

        // Partitioner
        // default for kafka >= 2.4 is sticky partitioner, below is round robin

        // Overriding compression to high throughput configuration
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentchange";

        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));

        EventSource eventSource = builder.build();

        // start the producer in another thread
        eventSource.start();

        // produce for 10 minutes
        try {
            TimeUnit.MINUTES.sleep(10);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
