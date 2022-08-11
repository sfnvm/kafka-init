package com.example.kafka.service;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Slf4j
@Service
public class ShortRunningService {
  static final String LOG_PATTERN = "Received topic: {} - partition: {} - offset: {}";

  @SneakyThrows
  public void run(String event, String topic, Integer partitionId, Long offset) {
    log.info(LOG_PATTERN, topic, partitionId, offset);
    log.info("Start short running job with event {}", event);
    Thread.sleep(Duration.ofSeconds(5).toMillis());
    log.info("Done short running job with event {}", event);
  }
}
