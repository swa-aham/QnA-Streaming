package com.kafka.consumer.service;

import com.kafka.consumer.entity.QnA;
import com.kafka.consumer.repository.QnARepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageListener {

    @Autowired
    private QnARepository qnaRepository;

    /*
    @KafkaListener(topics = "practice4", groupId = "qna-consumer-group")
    public void consumeQnAMessage(@Payload QnA qna, @Header(KafkaHeaders.OFFSET) long offset) {

        System.out.println("Received Message = " + qna.getQuestion() + " by consumer1");
    }
    */

    @KafkaListener(topics = "practice4", groupId = "qna-consumer-group-auto", containerFactory = "kafkaListenerContainerFactory")
    public void consumeQnAMessageAuto(ConsumerRecord<String, QnA> record) {

        QnA qna = record.value();
        System.out.println("Received Message = " + qna.getQuestion() + " by consumer2");

//        try {
//            // Process the message
//            System.out.println("QnA processed and saved successfully");
//
//        } catch (Exception e) {
//            System.err.println("Error in auto-acknowledge consumer: " + e.getMessage());
//            throw e; // Re-throw to trigger retry mechanism
//        }
    }
}