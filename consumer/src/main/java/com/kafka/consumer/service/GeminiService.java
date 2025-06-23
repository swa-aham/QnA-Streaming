package com.kafka.consumer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
public class GeminiService {

    @Value("${gemini.api.url}")
    private String apiUrl;

    @Value("${gemini.api.key}")
    private String apiKey;

    @Value("${gemini.max.retries:3}")
    private int maxRetries;

    @Value("${gemini.base.delay:10}")
    private int baseDelaySeconds;

    private final WebClient webClient;
    private final ObjectMapper objectMapper;

    public GeminiService(WebClient.Builder webClientBuilder, ObjectMapper objectMapper) {
        this.webClient = webClientBuilder
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(1024 * 1024))
                .build();
        this.objectMapper = objectMapper;
    }

    public String generateAnswer(String question) {
        try {
            String prompt = "Answer this history question in minimum words. Question: " + question;

            Map<String, Object> requestBody = Map.of(
                    "contents", new Object[]{
                            Map.of(
                                    "parts", new Object[]{
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
                    .retryWhen(createRetrySpec())
                    .onErrorResume(this::handleError)
                    .block();

            return extractText(response);

        } catch (Exception e) {
            System.err.println("Gemini API failed for question: " + question);
            e.printStackTrace();
            throw new RuntimeException("Failed to get response from Gemini after retries", e);
        }
    }

    private Retry createRetrySpec() {
        return Retry.backoff(maxRetries, Duration.ofSeconds(baseDelaySeconds))
                .filter(this::isRetryableError)
                .doBeforeRetry(retrySignal -> {
                    Throwable failure = retrySignal.failure();
                    long delay = getRetryDelay(failure);

                    System.out.println("Rate limit hit. Retry attempt: " + (retrySignal.totalRetries() + 1)
                            + "/" + maxRetries + ". Waiting " + delay + " seconds...");

                    if (delay > 0) {
                        try {
                            TimeUnit.SECONDS.sleep(delay);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException("Interrupted during retry delay", ie);
                        }
                    }
                })
                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
                    System.err.println("Max retries exceeded for Gemini API call");
                    return new RuntimeException("Gemini API rate limit exceeded after " + maxRetries + " retries",
                            retrySignal.failure());
                });
    }

    private boolean isRetryableError(Throwable throwable) {
        if (throwable instanceof WebClientResponseException) {
            WebClientResponseException webEx = (WebClientResponseException) throwable;
            // Retry on rate limiting (429) and server errors (5xx)
            return webEx.getStatusCode().value() == 429 ||
                    webEx.getStatusCode().is5xxServerError();
        }
        return false;
    }

    private long getRetryDelay(Throwable throwable) {
        if (throwable instanceof WebClientResponseException) {
            WebClientResponseException webEx = (WebClientResponseException) throwable;
            if (webEx.getStatusCode().value() == 429) {
                try {
                    // Try to extract retry delay from error response
                    String responseBody = webEx.getResponseBodyAsString();
                    JsonNode errorNode = objectMapper.readTree(responseBody);

                    JsonNode details = errorNode.path("error").path("details");
                    for (JsonNode detail : details) {
                        if ("type.googleapis.com/google.rpc.RetryInfo".equals(detail.path("@type").asText())) {
                            String retryDelay = detail.path("retryDelay").asText();
                            if (retryDelay.endsWith("s")) {
                                return Long.parseLong(retryDelay.substring(0, retryDelay.length() - 1));
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Failed to parse retry delay from error response: " + e.getMessage());
                }

                // Default delay for rate limiting
                return 45; // seconds
            }
        }
        return 0;
    }

    private Mono<String> handleError(Throwable throwable) {
        if (throwable instanceof WebClientResponseException) {
            WebClientResponseException webEx = (WebClientResponseException) throwable;

            if (webEx.getStatusCode().value() == 429) {
                System.err.println("Rate limit exceeded. Response: " + webEx.getResponseBodyAsString());
                return Mono.error(new RateLimitExceededException("Gemini API rate limit exceeded", webEx));
            } else if (webEx.getStatusCode().is4xxClientError()) {
                System.err.println("Client error: " + webEx.getStatusCode() + " - " + webEx.getResponseBodyAsString());
                return Mono.error(new GeminiApiException("Gemini API client error", webEx));
            }
        }

        return Mono.error(throwable);
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

    // Custom exceptions for better error handling
    public static class RateLimitExceededException extends RuntimeException {
        public RateLimitExceededException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static class GeminiApiException extends RuntimeException {
        public GeminiApiException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}