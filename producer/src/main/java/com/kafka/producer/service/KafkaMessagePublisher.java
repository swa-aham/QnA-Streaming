package com.kafka.producer.service;

import com.kafka.producer.entity.QnA;
import com.kafka.producer.exception.RateLimitException;
import com.kafka.producer.repository.QnARepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class KafkaMessagePublisher {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMessagePublisher.class);

    @Autowired
    private KafkaTemplate<String, QnA> template;

    @Autowired
    private GeminiService geminiService;

    @Autowired
    private QnARepository qnaRepository;

    private AtomicLong counter = new AtomicLong(1);
    private volatile long pauseUntilEpochMillis = 0;

    @Scheduled(fixedRate = 1500)
    public void publishMessageAfterEvery10Secs() {
        long now = System.currentTimeMillis();
        if (now < pauseUntilEpochMillis) {
            long secondsLeft = (pauseUntilEpochMillis - now) / 1000;
            logger.info("Producer paused. Resuming in {} seconds", secondsLeft);
            return;
        }

        Long id = counter.getAndIncrement();
        logger.debug("Generating question with ID: {}", id);

        try {
            String question = geminiService.generateQuestion();
            QnA qna = new QnA(id, question);

            try {
                qnaRepository.save(qna);
                logger.info("Question saved to database: {}", question);
            } catch (Exception e) {
                logger.error("Failed to save question to database: {}", question, e);
                return;
            }

            CompletableFuture<SendResult<String, QnA>> response = template.send("practice5", qna);
            response.whenComplete((result, ex) -> {
                if (ex == null) {
                    logger.info("Message sent successfully. Question: {}, Offset: {}",
                            question, result.getRecordMetadata().offset());
                } else {
                    logger.error("Failed to publish message. Question: {}, Error: {}",
                            question, ex.getMessage(), ex);
                }
            });

        } catch (RateLimitException e) {
            long waitMillis = e.getRetryAfterSeconds() * 1000L;
            pauseUntilEpochMillis = System.currentTimeMillis() + waitMillis;
            logger.warn("Rate limit hit. Pausing producer for {} seconds", e.getRetryAfterSeconds());
        } catch (Exception e) {
            logger.error("Unexpected error while generating or sending question", e);
        }
    }
}
