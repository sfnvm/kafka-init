package com.example.kafka.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class KafkaCommonConfig {
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

  public Map<String, Object> initConfig() {
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

  public Map<String, Object> adaptedConfigs() {
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
}
