package com.kafka.consumer.service;

import com.kafka.consumer.entity.QnA;
import com.kafka.consumer.repository.QnARepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ProcessingService {

    @Autowired
    private DatabaseService databaseService;

    @Autowired
    private GeminiService geminiService;

    @Autowired
    private QnARepository qnaRepository;

    public void processMessage(QnA qna) {

        String question = qna.getQuestion();
        String answer = geminiService.generateAnswer(question);
        qna.setAnswer(answer);
        qnaRepository.save(qna);
        System.out.println("Message: " + question + " # processed successfully.");
    }
}
