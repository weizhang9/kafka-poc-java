package com.github.weizhang9.kafka.rockingrobin;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    public TwitterProducer() {}

    private Logger logger = com.github.weizhang9.kafka1.Logger.createLogger(TwitterProducer.class);
    private String consumerKey = "key";
    private String consumerSecret = "secret";
    private String token = "token";
    private String tokenSecret = "secret";

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
        Client hosebirdClient = createTwitterClient(msgQueue);
        hosebirdClient.connect();

        Producer<String, String> producer = createProducer();

        while (!hosebirdClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.getCause();
                hosebirdClient.stop();
            } finally {
                producer.close();
            }

            if (msg != null) {
                logger.info(msg);
                producer.send(createProducerRecord("twitter_tweets", msg, null), (recordMetadata, e) -> {
                    if (e != null) {
                        logger.error("😬", e);
                    }
                });
            }
        }

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping programme...");
            logger.info("shutting down twitter client...");
            hosebirdClient.stop();
            logger.info("closing producer...");
            producer.close(); // flush data
            logger.info("ta-ra!");
        }));
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        List<String> terms = Lists.newArrayList("kotlin", "golang", "kafka");
        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    private static Producer<String, String> createProducer() {
        String bootstrapServers = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }

    private static ProducerRecord<String, String> createProducerRecord(String topic, String value, String key) {
        return key == null ? new ProducerRecord<>(topic, value) : new ProducerRecord<>(topic, key, value);
    }

    private static String recordMetaInfo(RecordMetadata recordMetadata) {
        return String.format("Received new metadata: \nTopic:%s\nPartition:%d\nOffset:%d\nTimestamp:%d",
                recordMetadata.topic(),
                recordMetadata.partition(),
                recordMetadata.offset(),
                recordMetadata.timestamp()
        );
    }
}
