package com.kafka.consumer.service;

import com.kafka.consumer.entity.QnA;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class KafkaMessageListener {

    @Autowired
    private ProcessingService processingService;

    @RetryableTopic(
            attempts = "4", // 3 retries + 1 original attempt
            backoff = @Backoff(delay = 60000, multiplier = 2.0), // Start with 1 minute, double each time
            autoCreateTopics = "true",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            retryTopicSuffix = "-retry",
            dltTopicSuffix = "-dlt",
            include = {ProcessingService.ProcessingRetryableException.class}
    )
    @KafkaListener(topics = "practice4", groupId = "qna-consumer-group-auto", containerFactory = "kafkaListenerContainerFactory")
    public void consumeAndProcessMessage(
            ConsumerRecord<String, QnA> record,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        QnA qna = record.value();
        System.out.println("Received Message = " + qna.getQuestion() +
                " from topic: " + topic +
                ", partition: " + partition +
                ", offset: " + offset);

        try {
            // Add artificial delay to respect rate limits
            addRateLimitDelay();

            // Process the message
            processingService.processMessage(qna);
            System.out.println("QnA processed and saved successfully");

        } catch (ProcessingService.ProcessingRetryableException e) {
            System.err.println("Retryable error in consumer for question: " + qna.getQuestion() +
                    ". Error: " + e.getMessage());
            throw e; // This will trigger the retry mechanism

        } catch (ProcessingService.ProcessingNonRetryableException e) {
            System.err.println("Non-retryable error in consumer for question: " + qna.getQuestion() +
                    ". Error: " + e.getMessage());
            // Don't re-throw, this will be considered processed

        } catch (Exception e) {
            System.err.println("Unexpected error in consumer: " + e.getMessage());
            e.printStackTrace();
            // Treat as non-retryable to avoid infinite loops
        }
    }

    // Handle messages that have exhausted all retries (Dead Letter Topic)
    @KafkaListener(topics = "practice4-dlt", groupId = "qna-consumer-group-dlt")
    public void handleDltMessage(ConsumerRecord<String, QnA> record) {
        QnA qna = record.value();
        System.err.println("Message moved to DLT after exhausting retries: " + qna.getQuestion());

        // Log to monitoring system, send alerts, or handle manually
        // You might want to save this to a separate table for manual processing
        try {
            qna.setAnswer("ERROR: Failed after all retries");
            qna.setProcessingStatus("DLT");
            // Save to database or send to monitoring system
            System.err.println("DLT message logged for manual review: " + qna.getQuestion());
        } catch (Exception e) {
            System.err.println("Failed to log DLT message: " + e.getMessage());
        }
    }

    /**
     * Add delay between messages to respect API rate limits
     * Gemini free tier: 15 requests per minute = 1 request every 4 seconds
     */
    private void addRateLimitDelay() {
        try {
            // Wait 5 seconds between requests to stay under the 15/minute limit
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during rate limit delay", e);
        }
    }
}