package com.kafka.consumer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Map;

@Service
public class GeminiService {

    @Value("${gemini.api.url}")
    private String apiUrl;

    @Value("${gemini.api.key}")
    private String apiKey;

    private final WebClient webClient;
    private final ObjectMapper objectMapper;

    public GeminiService(WebClient.Builder webClientBuilder, ObjectMapper objectMapper) {
        this.webClient = webClientBuilder.build();
        this.objectMapper = objectMapper;
    }

    public String generateAnswer(String question) {

        try {
            String prompt = "Answer this history question in minimum words. Question: " + question;

            // Query the AI model API
            Map<String, Object> requestBody = Map.of(
                    "contents", new Object[] {
                            Map.of(
                                    "parts", new Object[] {
                                            Map.of("text", prompt)
                                    }
                            )
                    }
            );

            String response = webClient.post()
                    .uri(apiUrl + apiKey)
                    .bodyValue(requestBody)
                    .retrieve()
                    .bodyToMono(String.class)
                    .block();

            return extractText(response);

        } catch (Exception e) {
            System.err.println("Gemini API failed for question: " + question);
            e.printStackTrace();
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
