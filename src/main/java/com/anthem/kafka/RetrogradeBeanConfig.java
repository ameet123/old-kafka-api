package com.anthem.kafka;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class RetrogradeBeanConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(RetrogradeBeanConfig.class);
    @Value("${zookeeper.urls}")
    private String zookeeperUrls;
    @Value("${kafka.topic}")
    private String kafkaTopic;
    private int topicThreads = 1;
    private final String GROUP_ID = "ameet-Retrograde-group-1";

    @Bean
    public ConsumerConfig createConsumerConfig() {
        System.out.println("Creating consumer with zoo URL:" + zookeeperUrls);
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeperUrls);
        props.put("group.id", GROUP_ID);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }

    @Bean
    public ExecutorService executor() {
        return Executors.newFixedThreadPool(topicThreads);
    }

    @Bean
    public ConsumerConnector consumerConnector() {
        return kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
    }

    @Bean
    public Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap() {
        Map<String, Integer> topicThreadMap = new HashMap<String, Integer>();
        topicThreadMap.put(kafkaTopic, topicThreads);
        return consumerConnector().createMessageStreams(topicThreadMap);
    }

    public void startFetcherThreads() {
        List<KafkaStream<byte[], byte[]>> streams = consumerMap().get(kafkaTopic);
        LOGGER.debug("# of message streams:{}  list of streams:{}", consumerMap().size(), streams.size());

        int threadNumber = 0;
        for (final KafkaStream<byte[], byte[]> stream : streams) {
            executor().submit(new MessageProcessor(stream, threadNumber));
            threadNumber++;
        }
    }
}
