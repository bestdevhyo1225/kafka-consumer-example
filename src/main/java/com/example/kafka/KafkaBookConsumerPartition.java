package com.example.kafka;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class KafkaBookConsumerPartition {

    private static final Logger logger = LoggerFactory.getLogger(KafkaBookConsumerPartition.class);

    public static void main(String[] args) {
        Properties props = new Properties();

        // 브로커 리스트를 정의한다.
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConsumerConfigValue.BOOTSTRAP_SERVERS);

        // 컨슈머에서 사용할 그룹 아이디를 지정한다.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, ConsumerGroupValue.HYO_PARTITION);

        // 백그라운드에서 주기적으로 오프셋을 수동으로 커밋한다.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ConsumerConfigValue.ENABLE_AUTO_COMMIT_FALSE);

        // latest 값을 적용해 토픽의 가장 마지막부터 메시지를 가져온다.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ConsumerConfigValue.AUTO_OFFSET_RESET_LATEST);

        // 메시지의 키와 값에 문자열을 사용했기 때문에 내장된 StringDeserializer 클래스를 지정한다.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // 새로운 컨슈머를 생성한다.
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 토픽의 파티션을 정의한다.
        TopicPartition partition0 = new TopicPartition(KafkaTopic.HYO_TOPIC, 0);
        TopicPartition partition1 = new TopicPartition(KafkaTopic.HYO_TOPIC, 1);

        try (consumer) {
            // 파티션0, 파티션1을 할당한다.
            consumer.assign(List.of(partition0, partition1));

            while (true) {
                // 할당된 파티션0, 파티션1의 메세지만 가져온다.
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    logger.info(
                        "Topic: {}, Partition: {}, Offset: {}, Key: {}, Value: {}",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value()
                    );
                }

                try {
                    consumer.commitSync();
                } catch (CommitFailedException exception) {
                    logger.error("exception", exception);
                }
            }
        }
    }
}
