package com.example.kafka;

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

public class KafkaBookConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaBookConsumer.class);

    public static void main(String[] args) {
        Properties props = new Properties();

        // 브로커 리스트를 정의한다.
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConsumerConfigValue.BOOTSTRAP_SERVERS);

        // 컨슈머에서 사용할 그룹 아이디를 지정한다.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, ConsumerGroupValue.HYO);

        // 백그라운드에서 주기적으로 오프셋을 자동으로 커밋한다.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ConsumerConfigValue.ENABLE_AUTO_COMMIT_TRUE);

        // latest 값을 적용해 토픽의 가장 마지막부터 메시지를 가져온다.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ConsumerConfigValue.AUTO_OFFSET_RESET_LATEST);

        // 브로커가 컨슈머가 살아있는 것으로 판단하기 위한 시간
        // 해당 시간내에 하트비트를 보내지 않으면, 해당 컨슈머는 종료되거나 장애가 발생한 것으로 판단하고 컨슈머 그룹은 '리밸런스' 를 한다.
        // 일반적으로 heartbeat.interval.ms 값이 함께 수정된다.
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, ConsumerConfigValue.SESSION_TIMEOUT);

        // Heartbeat 전송 시간 간격이다. HeartBeat 스레드는 heartbeat.interval.ms 간격으로 하트비트를 전송한다.
        // session.timeout.ms 값보다 낮아야 하며, 일반적으로 '3분의 1' 정도로 설정한다.
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, ConsumerConfigValue.HEARTBEAT_INTERVAL);

        // poll() 호출에 대한 최대 레코드 수 조정
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, ConsumerConfigValue.MAX_POLL_INTERVAL);

        // 주기적으로 poll() 을 호출하지 않으면 장애라고 판단하고 컨슈머 그룹에서 제외한 후, 컨슈머 그룹은 '리밸런스' 를 한다.
        // 컨슈머가 무한정 해당 파티션을 점유할 수 없도록 하기 위함이다.
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, ConsumerConfigValue.MAX_POLL_RECORDS);

        // 메시지의 키와 값에 문자열을 사용했기 때문에 내장된 StringDeserializer 클래스를 지정한다.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // 새로운 컨슈머를 생성한다.
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        /*
         * 컨슈머를 종료하기 전에 close() 메소드를 사용해서 네트워크 연결과 소켓을 종료한다.
         * 컨슈머가 하트비트를 보내지 않아 그룹 코디네이터에서 해당 컨슈머가 종료된 것으로 감지하는 것보다 빠르게 진행되며, 즉시 리밸런스가 일어난다.
         * */
        try (consumer) {
            // 메시지를 가져올 토픽을 구독하고, 리스트 형태로 여러 개의 토픽을 입력할 수 있다.
            consumer.subscribe(List.of(KafkaTopic.HYO_TOPIC));

            // 무한 루프이며, 메시지를 가져오기 위해 카프카에 지속적으로 poll() 요청을 하게된다.
            while (true) {
                try {
                    /*
                     * 컨슈머는 카프카에 Polling 하는 것을 계속 유지해야 한다. 그렇지 않으면, 종료된 것으로 간주되어 컨슈머에 할당된 파티션은
                     * 다른 컨슈머에게 전달되고, 새로운 컨슈머에 의해 메시지가 소비(컨슘)된다.
                     *
                     * Broker에 데이터를 가져오도록 요청하고 나서, Duration Timeout이 날 때까지 Broker로 부터 데이터를 가져오지 못하면,
                     * 즉시 Empty Collection을 반환한다.
                     * */
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    logger.info("records count: {}", records.count());

                    // 한 번에 하나의 메시지만 가져오는 것이 아니기 때문에 N개의 메시지 처리를 위해 반복문이 필요하다.
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info(
                            "Topic: {}, Partition: {}, Offset: {}, Key: {}, Value: {}",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value()
                        );
                    }
                } catch (Exception exception) {
                    logger.error("exception", exception);
                    break;
                }
            }
        }
    }
}
