package com.kafka.producer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.producer.exception.RateLimitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Map;

@Service
public class GeminiService {

    private static final Logger logger = LoggerFactory.getLogger(GeminiService.class);

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

    public String generateQuestion() {
        logger.debug("Generating a new question");

        try {
            String prompt = "Generate a single question which can be answered in one or two words. Question should related to some field or subject like history or science or politics.";

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
                                logger.warn("Gemini rate limit hit. Retry after {} seconds", retrySeconds);
                                return new RateLimitException("Rate limit exceeded", retrySeconds);
                            })
                    )
                    .bodyToMono(String.class)
                    .block();

            String question = extractText(response);
            logger.debug("Generated question: {}", question);
            return question;

        } catch (RateLimitException e) {
            logger.error("Rate limit exceeded while generating question");
            throw e;
        } catch (Exception e) {
            logger.error("Failed to generate question from Gemini API", e);
            throw new RuntimeException("Failed to get response from Gemini", e);
        }
    }

    private int extractRetryDelaySeconds(String body) {
        try {
            JsonNode json = objectMapper.readTree(body);
            for (JsonNode detail : json.path("error").path("details")) {
                if (detail.path("@type").asText().equals("type.googleapis.com/google.rpc.RetryInfo")) {
                    String delayStr = detail.path("retryDelay").asText();
                    return Integer.parseInt(delayStr.replace("s", ""));
                }
            }
        } catch (Exception e) {
            logger.error("Failed to extract retry delay from response", e);
        }
        logger.warn("Using fallback retry delay of 60 seconds");
        return 60;
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
