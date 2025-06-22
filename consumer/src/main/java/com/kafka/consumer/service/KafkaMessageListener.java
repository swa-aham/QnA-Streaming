package com.kafka.consumer.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageListener {

    @Autowired
    private ProcessingService processingService;

    @KafkaListener(topics = "practice3", groupId = "group1")
    public void consumeMessage(String message) {

        try {
            System.out.println("Consumed message : " + message);
            processingService.processMessage(message);
        } catch (Exception e) {
            System.out.println("Failed to process message. Exception: ");
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
