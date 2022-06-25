package com.example.kafka.linesplit;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class LineSplitOffsetCommitCallback implements OffsetCommitCallback {

    private static final Logger logger = LoggerFactory.getLogger(LineSplitOffsetCommitCallback.class);

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        if (exception != null) {
            logger.error("Commit failed for offsets: {}", offsets, exception);
        } else {
            logger.info("Commit succeeded");
        }
    }
}
