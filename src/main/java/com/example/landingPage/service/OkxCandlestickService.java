package com.example.landingPage.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.*;

@Service
public class OkxCandlestickService {

    private static final Logger logger = LoggerFactory.getLogger(OkxCandlestickService.class);

    private final StringRedisTemplate redisTemplate;
    private final SimpMessagingTemplate messagingTemplate;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ReactorNettyWebSocketClient client = new ReactorNettyWebSocketClient();
    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${okx.api.websocket-url}")
    private String websocketUrl;

    public OkxCandlestickService(StringRedisTemplate redisTemplate, SimpMessagingTemplate messagingTemplate) {
        this.redisTemplate = redisTemplate;
        this.messagingTemplate = messagingTemplate;
    }

    @PostConstruct
    public void init() {
        subscribeToCandlesticks();
    }

    public void subscribeToCandlesticks() {
        // Fetch valid OKX spot pairs
        List<String> validPairs = getValidOkxSpotPairs();
        if (validPairs.isEmpty()) {
            logger.warn("No valid OKX spot pairs found, skipping candlestick subscription");
            return;
        }

        String cmcData = redisTemplate.opsForValue().get("crypto:latest");
        if (cmcData == null) {
            logger.warn("No CMC data available, skipping candlestick subscription");
            return;
        }

        try {
            JsonNode root = mapper.readTree(cmcData);
            JsonNode dataArray = root.get("data");
            if (dataArray == null || !dataArray.isArray()) {
                logger.warn("Invalid CMC data structure");
                return;
            }

            List<String> topPairs = new ArrayList<>();
            for (int i = 0; i < 6 && i < dataArray.size(); i++) {
                String symbol = dataArray.get(i).get("symbol").asText();
                String instId = symbol + "-USDT";
                if (validPairs.contains(instId)) {
                    topPairs.add(instId);
                } else {
                    logger.warn("Skipping invalid OKX pair: {}", instId);
                }
            }

            if (topPairs.isEmpty()) {
                logger.warn("No valid top pairs found for candlestick subscription");
                return;
            }

            List<Map<String, String>> argsList = new ArrayList<>();
            for (String instId : topPairs) {
                Map<String, String> arg = new HashMap<>();
                arg.put("channel", "candle1H"); // 1-hour candlestick
                arg.put("instId", instId);
                argsList.add(arg);
            }

            Map<String, Object> subscribeMsg = new HashMap<>();
            subscribeMsg.put("op", "subscribe");
            subscribeMsg.put("args", argsList);

            String payload;
            try {
                payload = mapper.writeValueAsString(subscribeMsg);
                logger.info("Subscribing to candlesticks: {}", payload);
            } catch (JsonProcessingException e) {
                logger.error("Failed to serialize subscription message: {}", e.getMessage());
                return;
            }

            client.execute(
                    URI.create(websocketUrl),
                    session -> session.send(Mono.just(session.textMessage(payload)))
                            .thenMany(session.receive()
                                    .map(WebSocketMessage::getPayloadAsText)
                                    .doOnNext(message -> {
                                        logger.debug("Received WebSocket message: {}", message);
                                        handleCandlestickData(message);
                                    }))
                            .then()
            ).subscribe();

        } catch (JsonProcessingException e) {
            logger.error("Failed to process CMC data for subscription: {}", e.getMessage());
        } catch (Exception e) {
            logger.error("Unexpected error during candlestick subscription: {}", e.getMessage());
        }
    }

    private List<String> getValidOkxSpotPairs() {
        try {
            String url = "https://www.okx.com/api/v5/market/tickers?instType=SPOT";
            String response = restTemplate.getForObject(url, String.class);
            JsonNode root = mapper.readTree(response);
            JsonNode dataArray = root.get("data");
            if (dataArray == null || !dataArray.isArray()) {
                logger.warn("No valid spot pairs found in OKX API response");
                return Collections.emptyList();
            }

            List<String> pairs = new ArrayList<>();
            for (JsonNode ticker : dataArray) {
                String instId = ticker.get("instId").asText();
                if (instId.endsWith("-USDT")) {
                    pairs.add(instId);
                }
            }
            logger.info("Found {} valid OKX spot pairs", pairs.size());
            return pairs;
        } catch (Exception e) {
            logger.error("Failed to fetch OKX spot pairs: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    private void handleCandlestickData(String message) {
        try {
            JsonNode root = mapper.readTree(message);
            if (root.has("event") && "error".equals(root.get("event").asText())) {
                logger.warn("WebSocket error: {}", message);
                return;
            }

            JsonNode dataArray = root.get("data");
            JsonNode argNode = root.get("arg");
            if (dataArray == null || !dataArray.isArray() || dataArray.isEmpty() || argNode == null) {
                logger.debug("Skipping message with no valid data or arg: {}", message);
                return;
            }

            String instId = argNode.get("instId").asText();
            String symbol = instId.replace("-USDT", "");

            for (JsonNode candle : dataArray) {
                if (!candle.isArray() || candle.size() < 6) {
                    logger.warn("Invalid candlestick format for {}: {}", instId, candle);
                    continue;
                }

                long timestamp;
                double open, high, low, close, volume;
                try {
                    timestamp = candle.get(0).asLong();
                    open = candle.get(1).asDouble();
                    high = candle.get(2).asDouble();
                    low = candle.get(3).asDouble();
                    close = candle.get(4).asDouble(); // Fixed: Correct index for close
                    volume = candle.get(5).asDouble();
                    logger.debug("Processing candlestick for {}: open={}, close={}", instId, open, close);
                } catch (Exception e) {
                    logger.warn("Failed to parse candlestick values for {}: {}", instId, e.getMessage());
                    continue;
                }

                // Store candlestick in Redis
                Map<String, Object> candleData = new HashMap<>();
                candleData.put("timestamp", timestamp);
                candleData.put("open", open);
                candleData.put("high", high);
                candleData.put("low", low);
                candleData.put("close", close);
                candleData.put("volume", volume);

                String candleJson;
                try {
                    candleJson = mapper.writeValueAsString(candleData);
                    logger.debug("Storing candlestick for {}: {}", instId, candleJson);
                } catch (JsonProcessingException e) {
                    logger.error("Failed to serialize candlestick for {}: {}", instId, e.getMessage());
                    continue;
                }

                redisTemplate.opsForList().leftPush("candle:" + instId, candleJson);
                redisTemplate.opsForList().trim("candle:" + instId, 0, 23); // 24 hours

                // Calculate 24h % change
                double price24hAgo = open; // Fallback
                String oldestCandleJson = redisTemplate.opsForList().index("candle:" + instId, -1);
                if (oldestCandleJson != null) {
                    try {
                        JsonNode oldestCandle = mapper.readTree(oldestCandleJson);
                        price24hAgo = oldestCandle.get("open").asDouble();
                        logger.debug("Found oldest candlestick for {}: price24hAgo={}", instId, price24hAgo);
                    } catch (JsonProcessingException e) {
                        logger.warn("Failed to parse oldest candlestick for {}: {}", instId, e.getMessage());
                    }
                }

                double percentChange24h = price24hAgo != 0
                        ? ((close - price24hAgo) / price24hAgo) * 100
                        : 0.0;
                logger.debug("Calculated change24h for {}: {}%", instId, percentChange24h);

                // Store 24h change and close price
                redisTemplate.opsForValue().set("change24h:" + instId, String.format("%.2f", percentChange24h));
                redisTemplate.opsForValue().set("okx:" + instId + ":close", String.format("%.6f", close));

                // Prepare update for broadcast
                String logo = redisTemplate.opsForValue().get("logo:" + symbol);
                String circulatingSupply = redisTemplate.opsForValue().get("supply:" + symbol);

                Map<String, Object> update = new HashMap<>();
                update.put("pair", instId);
                update.put("price", close); // Use close as price for market cap
                update.put("change24h", percentChange24h);
                update.put("logo", logo);
                update.put("circulatingSupply", circulatingSupply != null ? Double.parseDouble(circulatingSupply) : null);
                update.put("timestamp", timestamp);

                try {
                    String updateJson = mapper.writeValueAsString(update);
                    redisTemplate.opsForValue().set("candle:latest:" + instId, updateJson);
                    logger.debug("Stored candle:latest for {}: {}", instId, updateJson);
                } catch (JsonProcessingException e) {
                    logger.error("Failed to serialize update for {}: {}", instId, e.getMessage());
                }

            }

        } catch (JsonProcessingException e) {
            logger.error("Failed to parse WebSocket message: {}. Error: {}", message, e.getMessage());
        } catch (Exception e) {
            logger.error("Unexpected error processing candlestick data: {}", e.getMessage());
        }
    }
}