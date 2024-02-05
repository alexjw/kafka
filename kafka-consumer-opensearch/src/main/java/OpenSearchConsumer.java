import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";
//        String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String,String> createKafkaConsumer() {
        // Create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        String groupId = "consumer-opensearch-demo";
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // If this is false, we manually handle the commits
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // Create a consumer
        return new KafkaConsumer<>(properties);
    }

    public static void main(String[] args) throws IOException {

        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        // OpenSearch Client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        // Create Kafka consumer
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
            consumer.wakeup();  // consumer.poll will throw a wakeupException

            // join the main threa to allow the execution of hte conde in the main thread
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }


        }));

        // Create the index on Opensearch if it doesn't exist already
        try (openSearchClient; consumer) {
            boolean wikimediaExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            if(!wikimediaExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The Wikimedia Index has been created");
            } else {
                log.info("The Wikimedia Index already exists");
            }

            consumer.subscribe(Collections.singleton("wikimedia.recentchange"));

            while (true) {
                ConsumerRecords<String , String > records = consumer.poll(Duration.ofMillis(3000));

                int recordCount = records.count();
                log.info("Received " + recordCount + " records");

                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> record : records) {
                    // send to opensearch

                    // Define and ID with kafka record coordinates, this will make the consumer effectively idempotent
                    // We can also extract the id from the record, it's inside the JSON
                    String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                    IndexRequest indexRequest = new IndexRequest("wikimedia").source(record.value(), XContentType.JSON).id(id);

                    bulkRequest.add(indexRequest);

                    //IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                    //log.info(response.getId());
                }


                if(bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted " + bulkResponse.getItems().length + " records");
                }
                Thread.sleep(1000); // To increase the amount of bulk

                // We commit only after we read all records, if ENABLE_AUTO_COMMIT_CONFIG is false
                consumer.commitSync();
                log.info("Offsets have been commited");
            }

        } catch (WakeupException e) {
            log.info("Consumer is starting to shutting down");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            consumer.close();
            openSearchClient.close();
            log.info("The consumer is gracefully shut down");
        }

    }
}
