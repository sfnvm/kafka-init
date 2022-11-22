package com.example.kafka.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
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

  @Value("${hddt.kafka.concurrency:10}")
  private Integer concurrency;

  @Autowired
  public KafkaInternalConfig(KafkaCommonConfig kafkaCommonConfig) {
    this.kafkaCommonConfig = kafkaCommonConfig;
  }

  @Bean
  public Map<String, Object> producerInternalConfigs() {
    Map<String, Object> props = kafkaCommonConfig.initConfig();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return props;
  }

  @Bean
  public ProducerFactory<String, String> producerInternalFactory() {
    return new DefaultKafkaProducerFactory<>(producerInternalConfigs());
  }

  @Bean
  public KafkaTemplate<String, String> kafkaInternalTemplate() {
    return new KafkaTemplate<>(producerInternalFactory());
  }

  @Bean
  public ConsumerFactory<String, String> consumerInternalFactory() {
    return new DefaultKafkaConsumerFactory<>(kafkaCommonConfig.adaptedConfigs());
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