package com.kafka.producer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.producer.exception.RateLimitException;
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

//    public String generateQuestion() {
//
//        try {
//            String prompt = "Generate a single question which can be answered in one or two words. Question should related to some field or subject like history or science or politics. .";
//
//            // Query the AI model API
//            Map<String, Object> requestBody = Map.of(
//                    "contents", new Object[] {
//                            Map.of(
//                                    "parts", new Object[] {
//                                            Map.of("text", prompt)
//                                    }
//                            )
//                    }
//            );
//
//            String response = webClient.post()
//                    .uri(apiUrl + apiKey)
//                    .bodyValue(requestBody)
//                    .retrieve()
//                    .bodyToMono(String.class)
//                    .block();
//
//            return extractText(response);
//        } catch (Exception e) {
//            System.err.println("Gemini API failed while generating question");
//            e.printStackTrace();
//            throw new RuntimeException(e);
//        }
//
//    }

    public String generateQuestion() {

        try {
            String prompt = "Generate a single question which can be answered in one or two words. Question should related to some field or subject like history or science or politics. .";

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
                    .onStatus(
                            status -> status.value() == 429,
                            clientResponse -> clientResponse.bodyToMono(String.class).map(body -> {
                                int retrySeconds = extractRetryDelaySeconds(body);
                                System.out.println("[WARNING] Gemini rate limit hit. Retry after " + retrySeconds + " seconds");
                                return new RateLimitException("Rate limit exceeded", retrySeconds);
                            })
                    )
                    .bodyToMono(String.class)
                    .block();

            return extractText(response);

        } catch (RateLimitException e) {
            throw e;
        } catch (Exception e) {
            System.err.println("Gemini API failed while generating question");
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private int extractRetryDelaySeconds(String body) {
        try {
            JsonNode json = objectMapper.readTree(body);
            for (JsonNode detail : json.path("error").path("details")) {
                if (detail.path("@type").asText().equals("type.googleapis.com/google.rpc.RetryInfo")) {
                    String delayStr = detail.path("retryDelay").asText(); // "55s"
                    return Integer.parseInt(delayStr.replace("s", ""));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 60; // fallback
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
