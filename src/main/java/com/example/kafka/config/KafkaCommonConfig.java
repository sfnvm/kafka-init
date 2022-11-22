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

  @Value("${hddt.kafka.external.bootstrap-servers}")
  private String bootstrapServers;

  public Map<String, Object> initConfig() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    // Security
    props.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol);
    props.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
    props.put(SaslConfigs.SASL_JAAS_CONFIG, saslJAAS);
    // Default DESERIALIZER
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    return props;
  }

  public Map<String, Object> adaptedConfigs() {
    Map<String, Object> props = initConfig();
    // Automatically reset the offset to the earliest offset
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return props;
  }
}
