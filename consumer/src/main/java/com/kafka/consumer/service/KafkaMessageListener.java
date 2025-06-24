package com.kafka.consumer.service;//package com.kafka.consumer.service;
//
//import com.kafka.consumer.entity.QnA;
//import com.kafka.consumer.exception.RateLimitException;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
//import org.springframework.kafka.listener.MessageListenerContainer;
//import org.springframework.stereotype.Service;
//
//@Service
//public class KafkaMessageListener {
//
//    @Autowired
//    private ProcessingService processingService;
//
//    @Autowired
//    private KafkaListenerEndpointRegistry registry;
//
//    private static final String LISTENER_ID = "qna-listener";
//
//    /*
//    @KafkaListener(topics = "practice4", groupId = "qna-consumer-group")
//    public void consumeQnAMessage(@Payload QnA qna, @Header(KafkaHeaders.OFFSET) long offset) {
//
//        System.out.println("Received Message = " + qna.getQuestion() + " by consumer1");
//    }
//    */
//
//    @KafkaListener(topics = "practice4", groupId = "qna-consumer-group-auto", containerFactory = "kafkaListenerContainerFactory")
//    public void consumeAndProcessMessage(ConsumerRecord<String, QnA> record) {
//
//        QnA qna = record.value();
//        System.out.println("Received Message = " + qna.getQuestion() + " by consumer2");
//
//        try {
//            processingService.processMessage(qna);
//            System.out.println("QnA processed and saved successfully");
//        } catch (RateLimitException e) {
//            System.err.println("Rate limit hit. Pausing consumer for " + e.getRetryAfterSeconds() + " seconds.");
//            pauseConsumerTemporarily(e.getRetryAfterSeconds());
//        } catch (Exception e) {
//            System.err.println("Error in auto-acknowledge consumer: " + e.getMessage());
//            throw e;
//        }
//    }
//
//    private void pauseConsumerTemporarily(int seconds) {
//        MessageListenerContainer container = registry.getListenerContainer(LISTENER_ID);
//        if (container != null && container.isRunning()) {
//            container.pause();
//            new Thread(() -> {
//                try {
//                    Thread.sleep(seconds * 1000L);
//                    container.resume();
//                    System.out.println("Consumer resumed after rate limit delay.");
//                } catch (InterruptedException e) {
//                    Thread.currentThread().interrupt();
//                }
//            }).start();
//        }
//    }
//}








import com.kafka.consumer.entity.QnA;
import com.kafka.consumer.exception.RateLimitException;
import com.kafka.consumer.service.ProcessingService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.kafka.;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageListener {

    @Autowired
    private ProcessingService processingService;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    private static final String LISTENER_ID = "qna-listener";

    @KafkaListener(id = LISTENER_ID, topics = "practice5", groupId = "qna-consumer-group-auto", containerFactory = "kafkaListenerContainerFactory")
    public void consumeAndProcessMessage(ConsumerRecord<String, QnA> record) {

        QnA qna = record.value();
        System.out.println("[INFO] Received Message: \"" + qna.getQuestion() + "\"");

        try {
            processingService.processMessage(qna);
            System.out.println("[INFO] QnA processed and saved successfully.");
        } catch (RateLimitException e) {
            System.out.println("[WARNING] Rate limit hit: " + e.getMessage());
            System.out.println("[INFO] Pausing consumer for " + e.getRetryAfterSeconds() + " seconds...");
            pauseConsumerTemporarily(e.getRetryAfterSeconds());
        } catch (Exception e) {
            System.err.println("[ERROR] Unexpected error during processing: " + e.getMessage());
            throw e;
        }
    }

    private void pauseConsumerTemporarily(int seconds) {
        MessageListenerContainer container = registry.getListenerContainer(LISTENER_ID);
        if (container != null && container.isRunning()) {
            container.pause();
            System.out.println("[INFO] Kafka consumer paused.");

            new Thread(() -> {
                try {
                    for (int i = seconds; i > 0; i--) {
                        System.out.print("\r[WAITING] Resuming in " + i + " seconds...");
                        Thread.sleep(1000);
                    }
                    System.out.println("\n[INFO] Resuming Kafka consumer now.");
                    container.resume();
                    System.out.println("[INFO] Kafka consumer resumed.");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.println("[ERROR] Resume delay interrupted.");
                }
            }).start();
        } else {
            System.err.println("[ERROR] Kafka consumer container not available or not running.");
        }
    }
}
