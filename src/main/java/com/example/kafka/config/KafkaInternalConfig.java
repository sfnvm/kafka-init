package com.example.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.Map;

@Configuration
@EnableKafka
public class KafkaInternalConfig {
  private final KafkaCommonConfig kafkaCommonConfig;

  /**
   * <h2>Consumer Config</h2>
   */
  @Value("${hddt.kafka.internal.consumer.group-id:message}")
  private String groupId;

  @Value("${hddt.kafka.internal.consumer.max-poll-records:30}")
  private Integer maxPollRecords;

  @Value("${hddt.kafka.internal.consumer.max-poll-interval:600000}")
  private Integer maxPollInterval;

  @Value("${hddt.kafka.internal.consumer.session-timeout:30000}")
  private Integer sessionTimeout;

  @Value("${hddt.kafka.internal.consumer.receive-buffer:1000000}")
  private Integer receiveBuffer;

  @Value("${hddt.kafka.internal.consumer.max-fetch-bytes:2000000}")
  private Integer maxFetchBytes;

  @Value("${hddt.kafka.internal.producer.max-request-size:2000000}")
  private Integer maxRequestSize;

  @Value("${hddt.kafka.concurrency:10}")
  private Integer concurrency;

  @Autowired
  public KafkaInternalConfig(KafkaCommonConfig kafkaCommonConfig) {
    this.kafkaCommonConfig = kafkaCommonConfig;
  }

  @Bean
  public Map<String, Object> producerInternalConfigs() {
    return kafkaCommonConfig.initConfig();
  }

  @Bean
  public ProducerFactory<String, String> producerInternalFactory() {
    return new DefaultKafkaProducerFactory<>(producerInternalConfigs());
  }

  @Bean
  public KafkaTemplate<String, String> kafkaInternalTemplate() {
    return new KafkaTemplate<>(producerInternalFactory());
  }

  @SuppressWarnings("Duplicates")
  @Bean
  public ConsumerFactory<String, String> consumerInternalFactory() {
    Map<String, Object> adaptedConfigs = kafkaCommonConfig.adaptedConfigs();
    adaptedConfigs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
    adaptedConfigs.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, receiveBuffer);
    adaptedConfigs.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval);
    adaptedConfigs.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);
    adaptedConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    adaptedConfigs.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxFetchBytes);
    adaptedConfigs.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, maxRequestSize);
    return new DefaultKafkaConsumerFactory<>(adaptedConfigs);
  }

  @Bean
  public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>>
  kafkaInternalListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, String> factory =
      new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerInternalFactory());
    factory.setConcurrency(concurrency);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    return factory;
  }
}