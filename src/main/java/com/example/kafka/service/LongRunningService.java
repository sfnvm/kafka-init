package com.example.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.concurrent.*;

@Service
@Slf4j
public class LongRunningService {
  static final String LOG_PATTERN = "Received topic: {} - partition: {} - offset: {}";

  public void run(String event, String topic, Integer partitionId, Long offset) {
    log.info(LOG_PATTERN, topic, partitionId, offset);

    log.info("Start long running job with event {}", event);
    try {
      Thread.sleep(Duration.ofMinutes(10).toMillis());
    } catch (InterruptedException e) {
      log.warn("Interrupted!", e);
      Thread.currentThread().interrupt();
    }
    log.info("Done long running job with event {}", event);
  }

  public void runWithCounter(String event, String topic, Integer partitionId, Long offset) {
    log.info(LOG_PATTERN, topic, partitionId, offset);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    CompletableFuture<Void> completableFuture = CompletableFuture.runAsync(() -> {
      log.info("Start long running job with event {}", event);
      try {
        Thread.sleep(Duration.ofMinutes(10).toMillis());
      } catch (InterruptedException e) {
        log.warn("Interrupted!", e);
        Thread.currentThread().interrupt();
      }
      log.info("Done long running job with event {}", event);
    }, executor);

    try {
      completableFuture.get(10, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      completableFuture.cancel(true);
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      log.warn("Interrupted!", e);
      Thread.currentThread().interrupt();
    }
  }
}
