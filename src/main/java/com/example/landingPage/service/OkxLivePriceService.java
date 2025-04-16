package com.example.landingPage.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.util.*;

@Service
public class OkxLivePriceService {

    private static final Logger logger = LoggerFactory.getLogger(OkxLivePriceService.class);

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ReactorNettyWebSocketClient client = new ReactorNettyWebSocketClient();

    @Value("${okx.api.websocket-url}")
    private String websocketUrl;

    public OkxLivePriceService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @PostConstruct
    public void init() {
        // Delay initialization until crypto:latest is available
        if (redisTemplate.opsForValue().get("crypto:latest") == null) {
            logger.info("Waiting for crypto:latest, retrying in 10s");
            Mono.delay(Duration.ofSeconds(10)).subscribe(v -> init());
            return;
        }
        subscribeToTopPopular();
    }

    public void subscribeToTopPopular() {
        // Fixed list of key pairs
        List<String> defaultPairs = Arrays.asList(
                "BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "BNB-USDT", "ADA-USDT",
                "PEPE-USDT", "SHIB-USDT", "DOGE-USDT", "BONK-USDT"
        );

        List<String> topPopular = new ArrayList<>(defaultPairs);

        // Add CMC top pairs
        String cmcData = redisTemplate.opsForValue().get("crypto:latest");
        if (cmcData != null) {
            try {
                JsonNode root = mapper.readTree(cmcData);
                JsonNode dataArray = root.get("data");
                if (dataArray != null && dataArray.isArray()) {
                    for (int i = 0; i < 10 && i < dataArray.size(); i++) {
                        String symbol = dataArray.get(i).get("symbol").asText();
                        String instId = symbol + "-USDT";
                        if (!topPopular.contains(instId)) {
                            topPopular.add(instId);
                        }
                    }
                }
            } catch (Exception e) {
                logger.warn("Failed to parse CMC data for subscriptions: {}", e.getMessage());
            }
        } else {
            logger.warn("No CMC data, using default pairs");
        }

        if (topPopular.isEmpty()) {
            logger.warn("No top pairs, retrying in 10s");
            Mono.delay(Duration.ofSeconds(10)).subscribe(v -> subscribeToTopPopular());
            return;
        }

        List<Map<String, String>> argsList = new ArrayList<>();
        for (String instId : topPopular) {
            Map<String, String> arg = new HashMap<>();
            arg.put("channel", "tickers");
            arg.put("instId", instId);
            argsList.add(arg);
        }

        Map<String, Object> subscribeMsg = new HashMap<>();
        subscribeMsg.put("op", "subscribe");
        subscribeMsg.put("args", argsList);

        String payload;
        try {
            payload = mapper.writeValueAsString(subscribeMsg);
            logger.info("Subscribing to tickers for {} pairs: {}", topPopular.size(), topPopular);
        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize subscription: {}", e.getMessage());
            Mono.delay(Duration.ofSeconds(10)).subscribe(v -> subscribeToTopPopular());
            return;
        }

        client.execute(
                URI.create(websocketUrl),
                session -> session.send(Mono.just(session.textMessage(payload)))
                        .doOnSuccess(v -> logger.info("Subscribed to tickers"))
                        .thenMany(session.receive()
                                .map(WebSocketMessage::getPayloadAsText)
                                .doOnNext(msg -> logger.debug("Received ticker: {}", msg))
                                .doOnNext(this::handleWebSocketData)
                                .doOnError(e -> logger.error("WebSocket receive error: {}", e.getMessage())))
                        .then()
        ).doOnError(e -> {
            logger.error("WebSocket connection error: {}", e.getMessage());
            Mono.delay(Duration.ofSeconds(5 + new Random().nextInt(10))).subscribe(v -> subscribeToTopPopular());
        }).doOnTerminate(() -> {
            logger.warn("WebSocket connection terminated, retrying in 5s");
            Mono.delay(Duration.ofSeconds(5)).subscribe(v -> subscribeToTopPopular());
        }).subscribe();
    }

    private void handleWebSocketData(String message) {
        try {
            JsonNode root = mapper.readTree(message);
            JsonNode dataArray = root.get("data");
            if (dataArray == null || !dataArray.isArray() || dataArray.isEmpty()) {
                logger.debug("No ticker data in message: {}", message);
                return;
            }

            String instId = root.get("arg").get("instId").asText();
            String symbol = instId.replace("-USDT", "");

            // Use ticker data as primary source
            JsonNode tickerData = dataArray.get(0);
            double price = tickerData.has("last") ? tickerData.get("last").asDouble() : 0.0;
            double open24h = tickerData.has("open24h") ? tickerData.get("open24h").asDouble() : 0.0;
            double change24h = open24h != 0 ? ((price - open24h) / open24h) * 100 : 0.0;
            double volume24h = tickerData.has("vol24h") ? tickerData.get("vol24h").asDouble() : 0.0;
            long timestamp = tickerData.has("ts") ? tickerData.get("ts").asLong() : System.currentTimeMillis();

            // Enhance with candlestick data if available
            String candleJson = redisTemplate.opsForValue().get("candle:latest:" + instId);
            if (candleJson != null) {
                try {
                    JsonNode candleData = mapper.readTree(candleJson);
                    if (candleData.has("price") && candleData.has("change24h") &&
                            candleData.has("timestamp") && candleData.has("volume24h")) {
                        price = candleData.get("price").asDouble();
                        change24h = Double.parseDouble(candleData.get("change24h").asText());
                        timestamp = candleData.get("timestamp").asLong();
                        volume24h = candleData.get("volume24h").asDouble();
                        logger.debug("Using candlestick data for {}", instId);
                    }
                } catch (Exception e) {
                    logger.warn("Failed to parse candlestick data for {}: {}", instId, e.getMessage());
                }
            }

            // Fetch metadata
            String logo = redisTemplate.opsForValue().get("logo:" + symbol);
            String circulatingSupply = redisTemplate.opsForValue().get("supply:" + symbol);
            Double circulatingSupplyValue = null;
            if (circulatingSupply != null) {
                try {
                    circulatingSupplyValue = Double.parseDouble(circulatingSupply);
                } catch (NumberFormatException e) {
                    logger.warn("Invalid circulating supply for {}: {}", symbol, circulatingSupply);
                }
            }

            // Construct update map
            Map<String, Object> update = new HashMap<>();
            update.put("pair", instId);
            update.put("price", price);
            update.put("change24h", String.format("%.2f", change24h));
            update.put("logo", logo); // Allow null
            update.put("circulatingSupply", circulatingSupplyValue); // Allow null
            update.put("timestamp", timestamp);
            update.put("volume24h", volume24h);

            // Store in Redis
            String updateJson = mapper.writeValueAsString(update);
            redisTemplate.opsForValue().set("popular:" + instId, updateJson, Duration.ofHours(24));
            redisTemplate.opsForValue().set("okx:" + instId, String.format("%.6f", price));
            logger.debug("Stored popular:{}: {}", instId, updateJson);

        } catch (Exception e) {
            logger.error("Error processing ticker: {}", e.getMessage());
        }
    }
}