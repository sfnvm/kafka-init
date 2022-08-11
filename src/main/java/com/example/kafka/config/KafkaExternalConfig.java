package com.example.kafka.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaExternalConfig {
  /**
   * <h2>Security</h2>
   */
  @Value("${hddt.kafka.security-protocol}")
  private String securityProtocol;

  @Value("${hddt.kafka.sasl.mechanism}")
  private String saslMechanism;

  @Value("${hddt.kafka.external.sasl.jaas-config}")
  private String saslJAAS;

  /**
   * <h2>Consumer Config</h2>
   */
  @Value("${hddt.kafka.external.bootstrap-servers}")
  private String bootstrapServers;

  @Value("${hddt.kafka.concurrency:10}")
  private Integer concurrency;

  @Value("${hddt.kafka.external.consumer.group-id:message}")
  private String groupId;

  @Value("${hddt.kafka.external.consumer.max-poll-records:30}")
  private Integer maxPollRecords;

  @Value("${hddt.kafka.external.consumer.max-poll-interval:600000}")
  private Integer maxPollInterval;

  @Value("${hddt.kafka.external.consumer.session-timeout:30000}")
  private Integer sessionTimeout;

  @Value("${hddt.kafka.external.consumer.receive-buffer:1000000}")
  private Integer receiveBuffer;

  @Value("${hddt.kafka.external.consumer.max-fetch-bytes:2000000}")
  private Integer maxFetchBytes;

  @Value("${hddt.kafka.external.producer.max-request-size:2000000}")
  private Integer maxRequestSize;

  @SuppressWarnings("Duplicates")
  private Map<String, Object> initConfig() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    // Security
    props.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol);
    props.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
    props.put(SaslConfigs.SASL_JAAS_CONFIG, saslJAAS);
    // Max size fetch
    props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxFetchBytes);
    props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, maxRequestSize);
    return props;
  }

  @Bean
  public Map<String, Object> producerExternalConfigs() {
    Map<String, Object> props = initConfig();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, maxRequestSize);
    return props;
  }

  @Bean
  public ProducerFactory<String, String> producerExternalFactory() {
    return new DefaultKafkaProducerFactory<>(producerExternalConfigs());
  }

  @Bean
  public KafkaTemplate<String, String> kafkaExternalTemplate() {
    return new KafkaTemplate<>(producerExternalFactory());
  }

  @SuppressWarnings("Duplicates")
  @Bean
  public Map<String, Object> consumerExternalConfigs() {
    Map<String, Object> props = initConfig();
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
    props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, receiveBuffer);
    props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval);
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);
    // Allows a pool of processes to divide the work of consuming and processing records
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    // Automatically reset the offset to the earliest offset
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Max size fetch
    props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxFetchBytes);
    return props;
  }

  @Bean
  public ConsumerFactory<String, String> consumerExternalFactory() {
    return new DefaultKafkaConsumerFactory<>(consumerExternalConfigs());
  }

  @Bean
  public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>>
  kafkaExternalListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, String> factory =
      new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerExternalFactory());
    factory.setConcurrency(2); // TODO: concurrency
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    return factory;
  }
}