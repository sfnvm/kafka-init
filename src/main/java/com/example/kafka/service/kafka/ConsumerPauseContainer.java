package com.example.kafka.service.kafka;

import com.example.kafka.service.LongRunningService;
import com.example.kafka.service.ProfileService;
import com.example.kafka.service.ShortRunningService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.task.AsyncListenableTaskExecutor;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

@Slf4j
@Service
@KafkaListener(
  topics = "pause_container_topic",
  id = "${kafka.container.id}",
  idIsGroup = false,
  containerFactory = "kafkaInternalListenerContainerFactory",
  groupId = "local-test"
)
public class ConsumerPauseContainer {
  private final LongRunningService longRunningService;
  private final ShortRunningService shortRunningService;
  private final AsyncListenableTaskExecutor executor;
  private final KafkaContainerSupportMethods containerSupportMethods;
  private final ProfileService profileService;

  @Value("${kafka.container.id}")
  private String containerId;

  static final String ERR_LOG_PATTERN = "Error payload @{} - topic: {} - partition: {} - offset: {}";

  @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
  @Autowired
  public ConsumerPauseContainer(
    LongRunningService longRunningService,
    ShortRunningService shortRunningService,
    AsyncListenableTaskExecutor executor,
    KafkaContainerSupportMethods containerSupportMethods,
    ProfileService profileService) {
    this.longRunningService = longRunningService;
    this.shortRunningService = shortRunningService;
    this.executor = executor;
    this.containerSupportMethods = containerSupportMethods;
    this.profileService = profileService;
  }

  @SuppressWarnings("Duplicates")
  @KafkaHandler
  public void handleEvent(
    @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
    @Header(KafkaHeaders.RECEIVED_PARTITION_ID) Integer partitionId,
    @Header(KafkaHeaders.OFFSET) Long offset,
    @Payload String event, Acknowledgment acknowledgment) {
    // log.info("Handling the event with body {} the pause container way", event);
    if (profileService.match("producer")) {
      // pauses the container
      containerSupportMethods.pauseConsume(containerId);
      ListenableFuture<?> listenableFuture = executor.submitListenable(() ->
        longRunningService.run(event, topic, partitionId, offset));
      listenableFuture.addCallback(result -> {
          acknowledgment.acknowledge();
          containerSupportMethods.resumeConsumer(containerId);
          log.info("Success callback");
        },
        ex -> {
          // perform retry mechanism like a dead letter queue here
          acknowledgment.acknowledge();
          containerSupportMethods.resumeConsumer(containerId);
          log.warn("Error callback");
        }
      );
    } else if (profileService.match("consumer")) {
      // pauses the container
      containerSupportMethods.pauseConsume(containerId);
      executor.submitListenable(() -> shortRunningService.run(event, topic, partitionId, offset))
        .addCallback(result -> {
            acknowledgment.acknowledge();
            containerSupportMethods.resumeConsumer(containerId);
            log.info("Success callback");
          },
          ex -> {
            acknowledgment.acknowledge();
            containerSupportMethods.resumeConsumer(containerId);
            log.warn("Error callback");
          }
        );
    }
  }
}
