package com.kafka.consumer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.consumer.exception.RateLimitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Map;

@Service
public class GeminiService2ForRetryTopic {

    private static final Logger logger = LoggerFactory.getLogger(GeminiService2ForRetryTopic.class);

    @Value("${gemini.api.url}")
    private String apiUrl;

    @Value("${gemini.api.key2}")
    private String apiKey;

    private final WebClient webClient;
    private final ObjectMapper objectMapper;

    public GeminiService2ForRetryTopic(WebClient.Builder webClientBuilder, ObjectMapper objectMapper) {
        this.webClient = webClientBuilder.build();
        this.objectMapper = objectMapper;
    }

    public String generateAnswer(String question) {
        logger.debug("{Retry Service} Generating answer for question: {}", question);

        try {
            String prompt = "Answer this history question in minimum words. Question: " + question;

            Map<String, Object> requestBody = Map.of(
                    "contents", new Object[]{
                            Map.of("parts", new Object[]{
                                    Map.of("text", prompt)
                            })
                    }
            );

            String response = webClient.post()
                    .uri(apiUrl + apiKey)
                    .bodyValue(requestBody)
                    .retrieve()
                    .bodyToMono(String.class)
                    .block();

            String answer = extractText(response);
            logger.debug("{Retry Service} Generated answer for question: {}, Answer: {}", question, answer);
            return answer;

        } catch (RateLimitException e) {
            logger.error("{Retry Service} Gemini rate-limited request for question: {}", question);
            throw e;
        } catch (Exception e) {
            logger.error("{Retry Service} Gemini API failed for question: {}", question, e);
            throw new RuntimeException("Failed to get response from Gemini", e);
        }
    }

    public static String extractText(String jsonString) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(jsonString);

        return root.path("candidates")
                .get(0)
                .path("content")
                .path("parts")
                .get(0)
                .path("text")
                .asText();
    }
}
