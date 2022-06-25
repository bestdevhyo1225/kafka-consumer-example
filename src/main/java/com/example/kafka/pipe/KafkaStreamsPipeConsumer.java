package com.example.kafka.pipe;

import com.example.kafka.KafkaTopic;
import java.time.Duration;
import java.util.List;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaStreamsPipeConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsPipeConsumer.class);

    public static void main(String[] args) {
        // 새로운 컨슈머를 생성한다.
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(KafkaStreamsPipeConsumerConfig.getProperties());

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

                try {
                    consumer.commitAsync();
                } catch (CommitFailedException exception) {
                    logger.error("exception", exception);
                }
            }
        }
    }
}
