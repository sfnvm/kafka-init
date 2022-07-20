package com.example.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

@Slf4j
@Service
public class ProducerService implements ApplicationRunner {
    private final KafkaTemplate<String, String> kafkaInternalTemplate;

    @Value("${hddt.kafka.send-timeout:60}")
    private long sendTimeout;

    @Value("${hddt.kafka.internal.consumer.topic:test}")
    private String testTopic;

    @Value("${hddt.kafka.dummy-produce:false}")
    private boolean dummyProduce;

    @Autowired
    public ProducerService(KafkaTemplate<String, String> kafkaInternalTemplate) {
        this.kafkaInternalTemplate = kafkaInternalTemplate;
    }

    @Retryable(
            maxAttempts = 5,
            backoff = @Backoff(delay = 5000, multiplier = 2),
            value = {TimeoutException.class}
    )
    public SendResult<String, String> send(String topic, String payload)
    throws ExecutionException, InterruptedException, TimeoutException {
        ListenableFuture<SendResult<String, String>> future = kafkaInternalTemplate.send(topic, payload);
        return future.get(sendTimeout, TimeUnit.SECONDS);
    }

    @Override
    public void run(ApplicationArguments args) {
        if (dummyProduce) {
            AtomicInteger counter = new AtomicInteger(1);

            final int poolSize = 500;

            Runnable task = () -> {
                if (counter.get() < 10000000) {
                    try {
                        send(testTopic, String.format("Message payload: %s", UUID.randomUUID()));
                        counter.incrementAndGet();
                        if (counter.get() % 100000 == 0) {
                            log.info("===BLOCK=== {}", counter.get());
                        }
                    } catch (ExecutionException | TimeoutException e) {
                        log.error(e.getMessage());
                    } catch (InterruptedException e) {
                        log.error(e.getMessage());
                        Thread.currentThread().interrupt();
                    }
                } else {
                    log.info("Done");

                }
            };

            ScheduledExecutorService executor = Executors.newScheduledThreadPool(poolSize);
            IntStream.range(0, poolSize).parallel().forEach(poolThreadNumb -> {
                log.info("Init producer in pool thread number {}", poolThreadNumb);
                executor.scheduleAtFixedRate(task, 0, 100, TimeUnit.NANOSECONDS);
            });
        }
    }
}
