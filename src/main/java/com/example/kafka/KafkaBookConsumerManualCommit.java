package com.example.kafka;

import org.apache.kafka.clients.consumer.CommitFailedException;
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

public class KafkaBookConsumerManualCommit {

    private static final Logger logger = LoggerFactory.getLogger(KafkaBookConsumerManualCommit.class);

    public static void main(String[] args) {
        Properties props = new Properties();

        // 브로커 리스트를 정의한다.
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConsumerConfigValue.BOOTSTRAP_SERVERS);

        // 컨슈머에서 사용할 그룹 아이디를 지정한다.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, ConsumerGroupValue.HYO_MANUAL);

        // 백그라운드에서 주기적으로 오프셋을 수동으로 커밋한다.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ConsumerConfigValue.ENABLE_AUTO_COMMIT_FALSE);

        // latest 값을 적용해 토픽의 가장 마지막부터 메시지를 가져온다.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ConsumerConfigValue.AUTO_OFFSET_RESET_LATEST);

        // 메시지의 키와 값에 문자열을 사용했기 때문에 내장된 StringDeserializer 클래스를 지정한다.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // 새로운 컨슈머를 생성한다.
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        try (consumer) {
            consumer.subscribe(List.of(KafkaTopic.HYO_TOPIC));

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

                try {
                    // insertIntoDB(data);
                    consumer.commitSync();
                } catch (CommitFailedException exception) {
                    logger.error("exception", exception);
                }
            }
        }
    }
}
