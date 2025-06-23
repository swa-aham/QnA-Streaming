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

        try {
            String answer = geminiService.generateAnswer(question);
            qna.setAnswer(answer);
            qnaRepository.save(qna);
            System.out.println("Message: " + question + " # processed successfully.");

        } catch (GeminiService.RateLimitExceededException e) {
            System.err.println("Rate limit exceeded for question: " + question);
            // Mark for retry or save with error status
            qna.setAnswer("ERROR: Rate limit exceeded");
            qna.setProcessingStatus("RATE_LIMITED"); // Add this field to your entity
            qnaRepository.save(qna);
            throw new ProcessingRetryableException("Rate limit exceeded", e);

        } catch (GeminiService.GeminiApiException e) {
            System.err.println("Gemini API error for question: " + question);
            qna.setAnswer("ERROR: API error");
            qna.setProcessingStatus("API_ERROR");
            qnaRepository.save(qna);
            throw new ProcessingRetryableException("Gemini API error", e);

        } catch (Exception e) {
            System.err.println("Unexpected error processing question: " + question);
            e.printStackTrace();
            qna.setAnswer("ERROR: Processing failed");
            qna.setProcessingStatus("FAILED");
            qnaRepository.save(qna);
            throw new ProcessingNonRetryableException("Processing failed", e);
        }
    }

    // Custom exceptions for Kafka retry logic
    public static class ProcessingRetryableException extends RuntimeException {
        public ProcessingRetryableException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static class ProcessingNonRetryableException extends RuntimeException {
        public ProcessingNonRetryableException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}