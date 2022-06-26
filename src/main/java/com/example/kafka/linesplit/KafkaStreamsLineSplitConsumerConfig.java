package com.example.kafka.linesplit;

import com.example.kafka.ConsumerConfigValue;
import com.example.kafka.ConsumerGroupValue;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

final class KafkaStreamsLineSplitConsumerConfig {

    public static Properties getProperties() {
        Properties props = new Properties();

        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, ConsumerConfigValue.ALLOW_AUTO_CREATE_TOPICS);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConsumerConfigValue.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, ConsumerGroupValue.HYO_STREAMS_LINE_SPLIT);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ConsumerConfigValue.ENABLE_AUTO_COMMIT_FALSE);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ConsumerConfigValue.AUTO_OFFSET_RESET_LATEST);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, ConsumerConfigValue.SESSION_TIMEOUT_MS);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, ConsumerConfigValue.HEARTBEAT_INTERVAL_MS);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, ConsumerConfigValue.MAX_POLL_INTERVAL_MS);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, ConsumerConfigValue.MAX_POLL_RECORDS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return props;
    }
}
