package com.kafka.consumer.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ProcessingService {

    @Autowired
    private DatabaseService databaseService;

    @Autowired
    private GeminiService geminiService;

    public void processMessage(String question) {

        String answer = geminiService.generateAnswer(question);
        databaseService.addAnswer(answer);
        System.out.println("Message: " + question + " # processed successfully.");
    }
}
