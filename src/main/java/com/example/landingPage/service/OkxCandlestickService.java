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
    private final ObjectMapper mapper = new ObjectMapper();
    private final ReactorNettyWebSocketClient client = new ReactorNettyWebSocketClient();
    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${okx.api.websocket-url}")
    private String websocketUrl;

    public OkxCandlestickService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @PostConstruct
    public void init() {
        updateCmcMetadata();
        fetchHistoricalCandlesticks();
        subscribeToCandlesticks();
    }

    private void updateCmcMetadata() {
        String cmcData = redisTemplate.opsForValue().get("crypto:latest");
        if (cmcData == null) {
            logger.warn("No CMC data available for metadata");
            return;
        }

        try {
            JsonNode root = mapper.readTree(cmcData);
            JsonNode dataArray = root.get("data");
            if (dataArray == null || !dataArray.isArray()) {
                logger.warn("Invalid CMC data format");
                return;
            }

            for (JsonNode coin : dataArray) {
                String symbol = coin.get("symbol").asText();
                String logo = coin.has("logo") ? coin.get("logo").asText() : null;
                String supply = coin.has("circulating_supply") ? coin.get("circulating_supply").asText() : null;
                if (logo != null) {
                    redisTemplate.opsForValue().set("logo:" + symbol, logo);
                }
                if (supply != null) {
                    redisTemplate.opsForValue().set("supply:" + symbol, supply);
                }
            }
            logger.info("Updated CMC metadata for {} coins", dataArray.size());
        } catch (Exception e) {
            logger.error("Error updating CMC metadata: {}", e.getMessage());
        }
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

        List<String> topPairs = getTopPairsByVolume(validPairs, 50);
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
            logger.info("Subscribing to candlesticks for {} pairs", topPairs.size());

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
                            logger.debug("Calculated change24h for {}: {}% (close={}, price24hAgo={})",
                                    instId, percentChange24h, close, price24hAgo);
                        } catch (JsonProcessingException e) {
                            logger.warn("Failed to parse oldest candlestick for {}: {}", instId, e.getMessage());
                        }
                    }
                } else {
                    logger.debug("Not enough candles for {} to calculate change24h (size={})", instId, size);
                    String tickerJson = redisTemplate.opsForValue().get("ticker:" + instId);
                    if (tickerJson != null) {
                        try {
                            JsonNode ticker = mapper.readTree(tickerJson);
                            double lastPrice = ticker.get("last").asDouble();
                            double open24h = ticker.get("open24h").asDouble();
                            percentChange24h = open24h != 0 ? ((lastPrice - open24h) / open24h) * 100 : 0.0;
                            logger.debug("Fallback change24h for {}: {}% (last={}, open24h={})",
                                    instId, percentChange24h, lastPrice, open24h);
                        } catch (Exception e) {
                            logger.warn("Failed to parse ticker for {}: {}", instId, e.getMessage());
                        }
                    }
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
                update.put("volume24h", volume);

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
            List<Map<String, Object>> tickers = new ArrayList<>();
            for (JsonNode ticker : root.get("data")) {
                String instId = ticker.get("instId").asText();
                if (instId.endsWith("-USDT")) {
                    pairs.add(instId);
                    Map<String, Object> tickerData = new HashMap<>();
                    tickerData.put("instId", instId);
                    tickerData.put("last", ticker.get("last").asDouble());
                    tickerData.put("open24h", ticker.get("open24h").asDouble());
                    tickerData.put("vol24h", ticker.get("vol24h").asDouble());
                    tickers.add(tickerData);
                    try {
                        redisTemplate.opsForValue().set("ticker:" + instId, mapper.writeValueAsString(tickerData));
                    } catch (JsonProcessingException e) {
                        logger.warn("Failed to store ticker for {}: {}", instId, e.getMessage());
                    }
                }
            }
            try {
                redisTemplate.opsForValue().set("tickers:latest", mapper.writeValueAsString(tickers));
                logger.info("Stored tickers:latest with {} entries", tickers.size());
            } catch (JsonProcessingException e) {
                logger.error("Failed to store tickers:latest: {}", e.getMessage());
            }
            logger.info("Fetched {} valid OKX spot pairs", pairs.size());
            return pairs;
        } catch (Exception e) {
            logger.error("Error fetching OKX pairs: {}", e.getMessage());
            return Arrays.asList("BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "BNB-USDT", "ADA-USDT");
        }
    }

    private List<String> getTopPairsByVolume(List<String> validPairs, int limit) {
        try {
            String url = "https://www.okx.com/api/v5/market/tickers?instType=SPOT";
            String response = restTemplate.getForObject(url, String.class);
            JsonNode root = mapper.readTree(response);
            List<Map<String, Object>> tickers = new ArrayList<>();
            for (JsonNode ticker : root.get("data")) {
                String instId = ticker.get("instId").asText();
                if (validPairs.contains(instId)) {
                    Map<String, Object> tickerData = new HashMap<>();
                    tickerData.put("instId", instId);
                    tickerData.put("vol24h", ticker.get("vol24h").asDouble());
                    tickers.add(tickerData);
                }
            }
            tickers.sort((a, b) -> Double.compare((Double) b.get("vol24h"), (Double) a.get("vol24h")));
            return tickers.stream()
                    .limit(limit)
                    .map(t -> (String) t.get("instId"))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            logger.error("Error fetching top pairs by volume: {}", e.getMessage());
            return validPairs.stream().limit(limit).collect(Collectors.toList());
        }
    }
}