package com.example.kafka.async;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class OffsetCommitAsyncCallback implements OffsetCommitCallback {

    private static final Logger logger = LoggerFactory.getLogger(OffsetCommitAsyncCallback.class);

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        if (exception != null) {
            logger.error("Commit failed for offsets: {}", offsets, exception);
        } else {
            logger.info("Commit succeeded");
        }
    }
}
