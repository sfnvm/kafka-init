package com.example.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ConsumerService {
    static final String LOG_PATTERN = "Received topic: {} - partition: {} - offset: {}";
    static final String PAYLOAD_LOG_PATTERN = "Payload @{}";

    @KafkaListener(
            topics = "#{'${hddt.kafka.internal.consumer.topic:test}'}",
            containerFactory = "kafkaInternalListenerContainerFactory"
    )
    public void receive(
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) Integer partitionId,
            @Header(KafkaHeaders.OFFSET) Long offset,
            @Payload String payload) {
        log.info(LOG_PATTERN, topic, partitionId, offset);
        log.info(PAYLOAD_LOG_PATTERN, payload);
    }

    @KafkaListener(
            topics = "#{'${hddt.kafka.internal.consumer.topic:highload_test}'}",
            containerFactory = "kafkaInternalListenerContainerFactory"
    )
    public void receiveHighPayload(
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) Integer partitionId,
            @Header(KafkaHeaders.OFFSET) Long offset,
            @Payload String payload) {
        log.info(LOG_PATTERN, topic, partitionId, offset);
        log.info(PAYLOAD_LOG_PATTERN, payload);
    }

    @KafkaListener(
            topicPattern = ".*_in",
            containerFactory = "kafkaExternalListenerContainerFactory"
    )
    public void receiveWildcard(
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) Integer partitionId,
            @Header(KafkaHeaders.OFFSET) Long offset,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) Long receivedTimestamp,
            @Payload String payload) {
        log.info(LOG_PATTERN, topic, partitionId, offset);
        log.info(PAYLOAD_LOG_PATTERN, payload);
    }
}
