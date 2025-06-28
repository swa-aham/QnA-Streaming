package com.kafka.consumer.service;

import com.kafka.consumer.entity.QnA;
import com.kafka.consumer.repository.QnARepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ProcessingService {

    private static final Logger logger = LoggerFactory.getLogger(ProcessingService.class);

    @Autowired
    private DatabaseService databaseService;

    @Autowired
    private GeminiService geminiService;

    @Autowired
    private GeminiService2ForRetryTopic geminiService2ForRetryTopic;

    @Autowired
    private QnARepository qnaRepository;

    public void processMessage(QnA qna, Boolean isRetry) {
        String question = qna.getQuestion();
        logger.debug("Processing question: {}", question);

        String answer;
        if(isRetry == false) {
            answer = geminiService.generateAnswer(question);
        } else {
            answer = geminiService2ForRetryTopic.generateAnswer(question);
        }
        qna.setAnswer(answer);

        try {
            qnaRepository.save(qna);
            logger.info("Successfully processed and saved Q&A. Question: {}", question);
        } catch (Exception e) {
            logger.error("Failed to save Q&A to repository. Question: {}", question, e);
            throw e;
        }
    }
}
