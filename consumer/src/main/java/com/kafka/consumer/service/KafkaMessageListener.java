package com.kafka.consumer.service;

import com.kafka.consumer.entity.QnA;
import com.kafka.consumer.exception.RateLimitException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessageListener {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageListener.class);
    private static final String LISTENER_ID = "qna-listener";
    private static final String LISTENER_ID2 = "qna-listener2";

    @Autowired
    private ProcessingService processingService;

    @Autowired
    private KafkaTemplate<String, QnA> template;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @KafkaListener(id = LISTENER_ID, topics = "practice7", groupId = "qna-consumer-group-auto", containerFactory = "kafkaListenerContainerFactory")
    public void consumeAndProcessMessage(ConsumerRecord<String, QnA> record) {
        QnA qna = record.value();

        if (qna == null) {
            logger.error("Skipping message due to deserialization error or null content. Raw record: {}", record);
            return;
        }

        logger.info("Received Message: \"{}\"", qna.getQuestion());

        try {
            processingService.processMessage(qna, false);
            logger.info("QnA with id:{} processed and saved successfully.", qna.getId());
        } catch (RateLimitException e) {
            logger.warn("Rate limit hit: {}", e.getMessage());

            CompletableFuture<SendResult<String, QnA>> response = template.send("level1-retry", qna);
            response.whenComplete((result, ex) -> {
                if (ex == null) {
                    logger.info("Message sent to retry topic. Id:{}, Question: {}, Offset: {}",
                            qna.getId(), qna.getQuestion(), result.getRecordMetadata().offset());
                } else {
                    logger.error("Failed to publish message to retry topic. ID : {}, Question: {}, Error: {}",
                            qna.getId(), qna.getQuestion(), ex.getMessage(), ex);
                }
            });

            logger.info("Pausing consumer for {} seconds...", e.getRetryAfterSeconds());
            pauseConsumerTemporarily(e.getRetryAfterSeconds());
        } catch (Exception e) {
            logger.error("Unexpected error during processing: {}", e.getMessage(), e);
            CompletableFuture<SendResult<String, QnA>> response = template.send("practice3.DLT", qna);
            response.whenComplete((result, ex) -> {
                if (ex == null) {
                    logger.info("Message sent to Dead Letter Topic. Id:{}, Question: {}, Offset: {}",
                            qna.getId(), qna.getQuestion(), result.getRecordMetadata().offset());
                } else {
                    logger.error("Failed to publish message to Dead Letter Topic. ID : {}, Question: {}, Error: {}",
                            qna.getId(), qna.getQuestion(), ex.getMessage(), ex);
                }
            });
            throw e;
        }
    }

    @KafkaListener(id = LISTENER_ID2, topics = "level1-retry", groupId = "qna-consumer-group-auto", containerFactory = "kafkaListenerContainerFactory")
    public void consumeFromRetryTopic(ConsumerRecord<String, QnA> record) {
        QnA qna = record.value();

        logger.info("In retry consumer, received Message: \"{}\"", qna.getQuestion());

        try {
            processingService.processMessage(qna, true);
            logger.info("{Retry topic} QnA with id:{} processed and saved successfully.", qna.getId());
        } catch (RateLimitException e) {
            logger.warn("{Retry topic} Rate limit hit: {}", e.getMessage());

            CompletableFuture<SendResult<String, QnA>> response = template.send("level1-retry", qna);
            response.whenComplete((result, ex) -> {
                if (ex == null) {
                    logger.info("{Retry topic} Message sent to retry topic. Id:{}, Question: {}, Offset: {}",
                            qna.getId(), qna.getQuestion(), result.getRecordMetadata().offset());
                } else {
                    logger.error("{Retry topic} Failed to publish message to retry topic. ID : {}, Question: {}, Error: {}",
                            qna.getId(), qna.getQuestion(), ex.getMessage(), ex);
                }
            });

            logger.info("{Retry topic} Pausing consumer for {} seconds...", e.getRetryAfterSeconds());
            pauseConsumerTemporarily(e.getRetryAfterSeconds());
        } catch (Exception e) {
            logger.error("{Retry topic} Unexpected error during processing: {}", e.getMessage(), e);
            CompletableFuture<SendResult<String, QnA>> response = template.send("practice3.DLT", qna);
            response.whenComplete((result, ex) -> {
                if (ex == null) {
                    logger.info("{Retry topic} Message sent to Dead Letter Topic. Id:{}, Question: {}, Offset: {}",
                            qna.getId(), qna.getQuestion(), result.getRecordMetadata().offset());
                } else {
                    logger.error("{Retry topic} Failed to publish message to Dead Letter Topic. ID : {}, Question: {}, Error: {}",
                            qna.getId(), qna.getQuestion(), ex.getMessage(), ex);
                }
            });
            throw e;
        }
    }

    private void pauseConsumerTemporarily(int seconds) {
        MessageListenerContainer container = registry.getListenerContainer(LISTENER_ID);
        if (container != null && container.isRunning()) {
            container.pause();
            logger.info("Kafka consumer paused.");

            new Thread(() -> {
                try {
                    for (int i = seconds; i > 0; i--) {
                        if (i % 10 == 0) { // Log only every 10 seconds to reduce noise
                            logger.info("Consumer will resume in {} seconds", i);
                        }
                        Thread.sleep(1000);
                    }
                    logger.info("Resuming Kafka consumer now.");
                    container.resume();
                    logger.info("Kafka consumer resumed.");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.error("Resume delay interrupted", e);
                }
            }).start();
        } else {
            logger.error("Kafka consumer container not available or not running.");
        }
    }
}
