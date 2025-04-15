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
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

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
        fetchHistoricalCandlesticks();
        subscribeToCandlesticks();
    }

    private void fetchHistoricalCandlesticks() {
        List<String> validPairs = getValidOkxSpotPairs();
        for (String instId : validPairs) {
            try {
                String url = "https://www.okx.com/api/v5/market/candles?instId=" + instId + "&bar=1H&limit=24";
                String response = restTemplate.getForObject(url, String.class);
                JsonNode root = mapper.readTree(response);
                JsonNode dataArray = root.get("data");
                if (dataArray == null || dataArray.isEmpty()) {
                    logger.warn("No historical candlesticks for {}", instId);
                    continue;
                }

                List<String> candles = new ArrayList<>();
                for (JsonNode candle : dataArray) {
                    Map<String, Object> candleData = new HashMap<>();
                    candleData.put("timestamp", candle.get(0).asLong());
                    candleData.put("open", candle.get(1).asDouble());
                    candleData.put("high", candle.get(2).asDouble());
                    candleData.put("low", candle.get(3).asDouble());
                    candleData.put("close", candle.get(4).asDouble());
                    candleData.put("volume", candle.get(5).asDouble());
                    candles.add(mapper.writeValueAsString(candleData));
                }
                for (int i = candles.size() - 1; i >= 0; i--) {
                    redisTemplate.opsForList().rightPush("candle:" + instId, candles.get(i));
                }
                redisTemplate.opsForList().trim("candle:" + instId, -24, -1);
                logger.info("Stored {} historical candlesticks for {}", candles.size(), instId);
            } catch (Exception e) {
                logger.error("Error fetching historical candlesticks for {}: {}", instId, e.getMessage());
            }
        }
    }

    public void subscribeToCandlesticks() {
        List<String> validPairs = getValidOkxSpotPairs();
        if (validPairs.isEmpty()) {
            logger.warn("No valid OKX spot pairs, retrying in 5s");
            Mono.delay(Duration.ofSeconds(5)).subscribe(v -> subscribeToCandlesticks());
            return;
        }

        List<String> topPairs = validPairs.stream()
                .filter(pair -> Arrays.asList("BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "BNB-USDT", "ADA-USDT").contains(pair))
                .collect(Collectors.toList());

        if (topPairs.isEmpty()) {
            logger.warn("No top pairs found, retrying in 5s");
            Mono.delay(Duration.ofSeconds(5)).subscribe(v -> subscribeToCandlesticks());
            return;
        }

        List<Map<String, String>> argsList = new ArrayList<>();
        for (String instId : topPairs) {
            Map<String, String> arg = new HashMap<>();
            arg.put("channel", "candle1H");
            arg.put("instId", instId);
            argsList.add(arg);
        }

        Map<String, Object> subscribeMsg = new HashMap<>();
        subscribeMsg.put("op", "subscribe");
        subscribeMsg.put("args", argsList);

        try {
            String payload = mapper.writeValueAsString(subscribeMsg);
            logger.info("Subscribing to candlesticks: {}", payload);

            client.execute(
                    URI.create(websocketUrl),
                    session -> session.send(Mono.just(session.textMessage(payload)))
                            .doOnSuccess(v -> logger.info("Subscribed to candlesticks"))
                            .thenMany(session.receive()
                                    .map(WebSocketMessage::getPayloadAsText)
                                    .doOnNext(msg -> logger.debug("Received candlestick: {}", msg))
                                    .doOnNext(this::handleCandlestickData)
                                    .doOnError(e -> logger.error("WebSocket receive error: {}", e.getMessage())))
                            .then()
            ).doOnError(e -> {
                logger.error("WebSocket connection error: {}", e.getMessage());
                Mono.delay(Duration.ofSeconds(5)).subscribe(v -> subscribeToCandlesticks());
            }).subscribe();

        } catch (Exception e) {
            logger.error("Subscription error: {}", e.getMessage());
            Mono.delay(Duration.ofSeconds(5)).subscribe(v -> subscribeToCandlesticks());
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
                logger.debug("Skipping invalid candlestick message: {}", message);
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
                    close = candle.get(4).asDouble();
                    volume = candle.get(5).asDouble();
                    logger.debug("Candlestick for {}: open={}, close={}", instId, open, close);
                } catch (Exception e) {
                    logger.warn("Failed to parse candlestick for {}: {}", instId, e.getMessage());
                    continue;
                }

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
                } catch (JsonProcessingException e) {
                    logger.error("Failed to serialize candlestick for {}: {}", instId, e.getMessage());
                    continue;
                }

                redisTemplate.opsForList().leftPush("candle:" + instId, candleJson);
                redisTemplate.opsForList().trim("candle:" + instId, 0, 23);

                double price24hAgo = 0.0;
                double percentChange24h = 0.0;
                Long size = redisTemplate.opsForList().size("candle:" + instId);
                if (size != null && size >= 24) {
                    String oldestCandleJson = redisTemplate.opsForList().index("candle:" + instId, 23);
                    if (oldestCandleJson != null) {
                        try {
                            JsonNode oldestCandle = mapper.readTree(oldestCandleJson);
                            price24hAgo = oldestCandle.get("open").asDouble();
                            percentChange24h = price24hAgo != 0 ? ((close - price24hAgo) / price24hAgo) * 100 : 0.0;
                            logger.debug("change24h for {}: {}% (price24hAgo={}, close={})", instId, percentChange24h, price24hAgo, close);
                        } catch (JsonProcessingException e) {
                            logger.warn("Failed to parse oldest candlestick for {}: {}", instId, e.getMessage());
                        }
                    }
                } else {
                    logger.debug("Not enough candlesticks for {}: size={}", instId, size);
                }

                String formattedChange = String.format("%.2f", percentChange24h);
                redisTemplate.opsForValue().set("change24h:" + instId, formattedChange);
                redisTemplate.opsForValue().set("okx:" + instId + ":close", String.format("%.6f", close));

                String logo = redisTemplate.opsForValue().get("logo:" + symbol);
                String circulatingSupply = redisTemplate.opsForValue().get("supply:" + symbol);

                Map<String, Object> update = new HashMap<>();
                update.put("pair", instId);
                update.put("price", close);
                update.put("change24h", Double.parseDouble(formattedChange));
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

        } catch (Exception e) {
            logger.error("Error processing candlestick: {}", e.getMessage());
        }
    }

    private List<String> getValidOkxSpotPairs() {
        try {
            String url = "https://www.okx.com/api/v5/market/tickers?instType=SPOT";
            String response = restTemplate.getForObject(url, String.class);
            JsonNode root = mapper.readTree(response);
            List<String> pairs = new ArrayList<>();
            for (JsonNode ticker : root.get("data")) {
                String instId = ticker.get("instId").asText();
                if (instId.endsWith("-USDT")) {
                    pairs.add(instId);
                }
            }
            return pairs;
        } catch (Exception e) {
            logger.error("Error fetching OKX pairs: {}", e.getMessage());
            return List.of("BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT");
        }
    }
}