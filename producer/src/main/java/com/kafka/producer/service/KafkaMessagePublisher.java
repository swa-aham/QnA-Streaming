package com.kafka.producer.service;

import com.kafka.producer.entity.QnA;
import com.kafka.producer.repository.QnARepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class KafkaMessagePublisher {

    @Autowired
    private KafkaTemplate<String, QnA> template;

    @Autowired
    private GeminiService geminiService;

    @Autowired
    private QnARepository qnaRepository;

    private AtomicLong counter = new AtomicLong(1);

    /*
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
    */

    @Scheduled(fixedRate = 1500)
    public void publishMessageAfterEvery10Secs() {

        Long id = counter.getAndIncrement();
        String question = geminiService.generateQuestion();
        QnA qna = new QnA();
        qna.setId(id);
        qna.setQuestion(question);
        qnaRepository.save(qna);
        System.out.println("Question: " + question + " added to database");
        CompletableFuture<SendResult<String, QnA>> response = template.send("practice4", qna);
        response.whenComplete((result, ex) -> {
            if(ex == null) {
                System.out.println("Sent message = [" + question + "] with offset = [" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to publish message = [" + question + "] due to: " + ex.getMessage());
            }
        });
    }
}
