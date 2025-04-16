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
        subscribeToTopPopular();
    }

    public void subscribeToTopPopular() {
        String cmcData = redisTemplate.opsForValue().get("crypto:latest");
        if (cmcData == null) {
            logger.warn("No CMC data, retrying in 5s");
            Mono.delay(Duration.ofSeconds(5)).subscribe(v -> subscribeToTopPopular());
            return;
        }

        try {
            JsonNode root = mapper.readTree(cmcData);
            JsonNode dataArray = root.get("data");
            if (dataArray == null || !dataArray.isArray()) {
                logger.warn("Invalid CMC data");
                return;
            }

            List<String> topPopular = new ArrayList<>();
            for (int i = 0; i < 10 && i < dataArray.size(); i++) {
                String symbol = dataArray.get(i).get("symbol").asText();
                String instId = symbol + "-USDT";
                topPopular.add(instId);
            }

            if (topPopular.isEmpty()) {
                logger.warn("No top pairs found, retrying in 5s");
                Mono.delay(Duration.ofSeconds(5)).subscribe(v -> subscribeToTopPopular());
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
                logger.info("Subscribing to tickers: {}", payload);
            } catch (JsonProcessingException e) {
                logger.error("Failed to serialize subscription: {}", e.getMessage());
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
                Mono.delay(Duration.ofSeconds(5)).subscribe(v -> subscribeToTopPopular());
            }).subscribe();

        } catch (Exception e) {
            logger.error("Subscription error: {}", e.getMessage());
            Mono.delay(Duration.ofSeconds(5)).subscribe(v -> subscribeToTopPopular());
        }
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

            String candleJson = redisTemplate.opsForValue().get("candle:latest:" + instId);
            if (candleJson == null) {
                logger.debug("No candlestick data for {}, skipping", instId);
                return;
            }

            JsonNode candleData = mapper.readTree(candleJson);
            double okxPrice = candleData.get("price").asDouble();
            double change24h = candleData.get("change24h").asDouble();
            long timestamp = candleData.get("timestamp").asLong();
            double volume24h = candleData.has("volume24h") ? candleData.get("volume24h").asDouble() : 0.0;

            redisTemplate.opsForValue().set("okx:" + instId, String.format("%.6f", okxPrice));

            String logo = redisTemplate.opsForValue().get("logo:" + symbol);
            String circulatingSupply = redisTemplate.opsForValue().get("supply:" + symbol);

            Map<String, Object> update = new HashMap<>();
            update.put("pair", instId);
            update.put("price", okxPrice);
            update.put("change24h", change24h);
            update.put("logo", logo);
            update.put("circulatingSupply", circulatingSupply != null ? Double.parseDouble(circulatingSupply) : null);
            update.put("timestamp", timestamp);
            update.put("volume24h", volume24h);

            String updateJson = mapper.writeValueAsString(update);
            redisTemplate.opsForValue().set("popular:" + instId, updateJson);
            logger.debug("Stored popular:{}: {}", instId, updateJson);

        } catch (Exception e) {
            logger.error("Error processing ticker: {}", e.getMessage());
        }
    }
}