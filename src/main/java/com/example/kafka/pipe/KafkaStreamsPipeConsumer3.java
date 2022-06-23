package com.example.kafka.pipe;

import com.example.kafka.ConsumerConfigValue;
import com.example.kafka.ConsumerGroupValue;
import com.example.kafka.KafkaTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class KafkaStreamsPipeConsumer3 {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsPipeConsumer3.class);

    public static void main(String[] args) {
        Properties props = new Properties();

        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, ConsumerConfigValue.ALLOW_AUTO_CREATE_TOPICS);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConsumerConfigValue.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, ConsumerGroupValue.HYO_STREAMS);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ConsumerConfigValue.ENABLE_AUTO_COMMIT_TRUE);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ConsumerConfigValue.AUTO_OFFSET_RESET_LATEST);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, ConsumerConfigValue.SESSION_TIMEOUT);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, ConsumerConfigValue.HEARTBEAT_INTERVAL);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, ConsumerConfigValue.MAX_POLL_INTERVAL);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, ConsumerConfigValue.MAX_POLL_RECORDS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // 새로운 컨슈머를 생성한다.
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        try (consumer) {
            consumer.subscribe(List.of(KafkaTopic.STREAMS_PIPE_OUTPUT));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                logger.info("records count: {}", records.count());

                // 한 번에 하나의 메시지만 가져오는 것이 아니기 때문에 N개의 메시지 처리를 위해 반복문이 필요하다.
                for (ConsumerRecord<String, String> record : records) {
                    logger.info(
                        "Topic: {}, Partition: {}, Offset: {}, Key: {}, Value: {}",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value()
                    );
                }
            }
        }
    }
}
