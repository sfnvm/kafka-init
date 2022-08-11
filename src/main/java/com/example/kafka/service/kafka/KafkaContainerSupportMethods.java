package com.example.kafka.service.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class KafkaContainerSupportMethods {
  private final KafkaListenerEndpointRegistry registry;

  @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
  @Autowired
  public KafkaContainerSupportMethods(KafkaListenerEndpointRegistry registry) {
    this.registry = registry;
  }

  public void pauseConsume(String containerId) {
    getContainer(containerId).ifPresent(MessageListenerContainer::pause);
  }

  public void resumeConsumer(String containerId) {
    getContainer(containerId).ifPresent(MessageListenerContainer::resume);
  }

  private Optional<MessageListenerContainer> getContainer(String containerId) {
    return Optional.ofNullable(registry.getListenerContainer(containerId));
  }
}
