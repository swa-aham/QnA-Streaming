package com.kafka.producer.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class KafkaMessagePublisher {

    @Autowired
    private KafkaTemplate<String, Object> template;

    @Autowired
    private GeminiService geminiService;

    @Autowired
    private DatabaseService databaseService;

    private AtomicInteger counter = new AtomicInteger(1);

    public void publishMessage(String message) {

        CompletableFuture<SendResult<String, Object>> response = template.send("practice", message);
        response.whenComplete((result, ex) -> {
            if(ex == null) {
                System.out.println("Sent message = [" + message + "] with offset = [" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to publish message = [" + message + "] due to: " + ex.getMessage());
            }
        });
    }

    @Scheduled(fixedRate = 5000)
    public void publishMessageAfterEvery10Secs() {

        String question = geminiService.generateQuestion();
        databaseService.addQuestion(question);
        System.out.println("Question: " + question + " added to database");
        String message = question;
        CompletableFuture<SendResult<String, Object>> response = template.send("practice3", message);
        response.whenComplete((result, ex) -> {
            if(ex == null) {
                System.out.println("Sent message = [" + message + "] with offset = [" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to publish message = [" + message + "] due to: " + ex.getMessage());
            }
        });
    }
}
