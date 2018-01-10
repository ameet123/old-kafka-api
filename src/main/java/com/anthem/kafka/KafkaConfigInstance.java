package com.anthem.kafka;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

//@Component
public class KafkaConfigInstance {
    private final ConsumerConnector consumer;
    private final String topic;
    private ExecutorService executor;

    public KafkaConfigInstance() {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig("dwbdtest1r1m.wellpoint.com:2181/kafka", "ameet-retrograde-1"));
        this.topic = "anthem.kf";
    }

    private ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }

    public void run(int a_numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, a_numThreads);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        System.out.println("# of message streams:" + consumerMap.size() + " list of streams:" + streams.size());
        executor = Executors.newFixedThreadPool(a_numThreads);

        int threadNumber = 0;
        for (final KafkaStream<byte[], byte[]> stream : streams) {
            executor.submit(new MessageProcessor(stream, threadNumber));
            threadNumber++;
        }
    }
}
