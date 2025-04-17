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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
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
        logger.info("Initializing OkxCandlestickService");
        fetchHistoricalCandlesticks();
        subscribeToCandlesticks();
    }

    private void fetchHistoricalCandlesticks() {
        logger.info("Fetching historical candlesticks for all valid pairs");
        List<String> validPairs = getValidOkxSpotPairs();
        for (String instId : validPairs) {
            try {
                String url = "https://www.okx.com/api/v5/market/candles?instId=" + instId + "&bar=1m&limit=60";
                String response = restTemplate.getForObject(url, String.class);
                if (response == null) {
                    logger.warn("No response for historical candlesticks for {}", instId);
                    continue;
                }
                logger.debug("Historical candlesticks response for {}: {}", instId, response);

                JsonNode root = mapper.readTree(response);
                JsonNode dataArray = root.get("data");
                if (dataArray == null || dataArray.isEmpty()) {
                    logger.warn("No historical candlesticks for {}", instId);
                    continue;
                }

                Map<String, String> candles = new HashMap<>();
                String latestCandleJson = null;
                long latestTimestamp = 0;

                for (JsonNode candle : dataArray) {
                    String timestamp = candle.get(0).asText();
                    Map<String, Object> candleData = new HashMap<>();
                    double open = candle.get(1).asDouble();
                    double high = candle.get(2).asDouble();
                    double low = candle.get(3).asDouble();
                    double close = candle.get(4).asDouble();
                    double volume = candle.get(5).asDouble();
                    candleData.put("open", open);
                    candleData.put("high", high);
                    candleData.put("low", low);
                    candleData.put("close", close);
                    candleData.put("volume", volume);
                    candleData.put("price", close);
                    candleData.put("timestamp", Long.parseLong(timestamp));

                    // Fetch 24h change and volume from TICKER:{PAIR}
                    String tickerJson = redisTemplate.opsForValue().get("TICKER:" + instId);
                    double change24h = 0.0;
                    double volume24h = 0.0;
                    if (tickerJson != null) {
                        JsonNode ticker = mapper.readTree(tickerJson);
                        double lastPrice = ticker.get("last").asDouble();
                        double open24h = ticker.get("open24h").asDouble();
                        change24h = open24h != 0 ? ((lastPrice - open24h) / open24h) * 100 : 0.0;
                        volume24h = ticker.get("vol24h").asDouble();
                    }
                    candleData.put("change24h", String.format("%.2f", change24h));
                    candleData.put("volume24h", volume24h);

                    // Fetch logo and circulating supply
                    String symbol = instId.replace("-USDT", "");
                    Object logoObj = redisTemplate.opsForHash().get("COININFO:" + symbol, "logo");
                    Object circulatingSupplyObj = redisTemplate.opsForHash().get("COININFO:" + symbol, "circulating_supply");
                    String logo = logoObj != null ? logoObj.toString() : "https://default.logo.png";
                    String circulatingSupply = circulatingSupplyObj != null ? circulatingSupplyObj.toString() : "0";
                    double circSupply;
                    try {
                        circSupply = Double.parseDouble(circulatingSupply);
                    } catch (NumberFormatException e) {
                        logger.warn("Invalid circulating_supply for {}: {}", symbol, circulatingSupply);
                        circSupply = 0.0;
                    }
                    candleData.put("logo", logo);
                    candleData.put("circulatingSupply", circSupply);

                    String candleJson = mapper.writeValueAsString(candleData);
                    candles.put(timestamp, candleJson);

                    // Track the latest candlestick
                    long candleTimestamp = Long.parseLong(timestamp);
                    if (candleTimestamp > latestTimestamp) {
                        latestTimestamp = candleTimestamp;
                        latestCandleJson = candleJson;
                    }
                }

                // Store in CANDLES:{PAIR}:1m
                redisTemplate.opsForHash().putAll("CANDLES:" + instId + ":1m", candles);
                redisTemplate.expire("CANDLES:" + instId + ":1m", 1, TimeUnit.HOURS);

                // Store latest candlestick
                if (latestCandleJson != null) {
                    redisTemplate.opsForValue().set("candle:latest:" + instId, latestCandleJson, 1, TimeUnit.HOURS);
                    logger.debug("Stored latest candlestick for {}: {}", instId, latestCandleJson);
                }

                redisTemplate.opsForSet().add("active:pairs", instId);
                logger.info("Stored {} historical 1m candlesticks for {}", candles.size(), instId);
            } catch (JsonProcessingException e) {
                logger.error("JSON parsing error for historical candlesticks for {}: {}", instId, e.getMessage(), e);
            } catch (Exception e) {
                logger.error("Error fetching historical candlesticks for {}: {}", instId, e.getMessage(), e);
            }
        }
    }

    public void subscribeToCandlesticks() {
        Set<String> activePairs = redisTemplate.opsForSet().members("active:pairs");
        if (activePairs == null || activePairs.isEmpty()) {
            logger.warn("No active pairs in active:pairs, retrying in 5s");
            Mono.delay(Duration.ofSeconds(5)).subscribe(v -> subscribeToCandlesticks());
            return;
        }

        List<String> topPairs = getTopPairsByVolume(new ArrayList<>(activePairs), 50);
        if (topPairs.isEmpty()) {
            logger.warn("No top pairs found, retrying in 5s");
            Mono.delay(Duration.ofSeconds(5)).subscribe(v -> subscribeToCandlesticks());
            return;
        }

        List<Map<String, String>> argsList = new ArrayList<>();
        for (String instId : topPairs) {
            Map<String, String> arg = new HashMap<>();
            arg.put("channel", "candle1m");
            arg.put("instId", instId);
            argsList.add(arg);
        }

        Map<String, Object> subscribeMsg = new HashMap<>();
        subscribeMsg.put("op", "subscribe");
        subscribeMsg.put("args", argsList);

        try {
            String payload = mapper.writeValueAsString(subscribeMsg);
            logger.info("Subscribing to 1m candlesticks for {} pairs: {}", topPairs.size(), topPairs);

            client.execute(
                    URI.create(websocketUrl),
                    session -> {
                        logger.info("WebSocket connected to {}", websocketUrl);
                        logger.info("Sending subscription payload: {}", payload);
                        return session.send(Mono.just(session.textMessage(payload)))
                                .doOnSuccess(v -> logger.info("Subscription sent successfully"))
                                .thenMany(session.receive()
                                        .map(WebSocketMessage::getPayloadAsText)
                                        .doOnNext(msg -> logger.debug("Received WebSocket message: {}", msg))
                                        .doOnNext(this::handleCandlestickData)
                                        .doOnError(e -> logger.error("WebSocket receive error: {}", e.getMessage())))
                                .then();
                    }
            ).doOnError(e -> {
                logger.error("WebSocket connection failed: {}", e.getMessage(), e);
                long delay = 5 + ThreadLocalRandom.current().nextLong(10);
                logger.info("Retrying subscription in {} seconds", delay);
                Mono.delay(Duration.ofSeconds(delay)).subscribe(v -> subscribeToCandlesticks());
            }).subscribe();

        } catch (JsonProcessingException e) {
            logger.error("JSON serialization error for subscription: {}", e.getMessage(), e);
            Mono.delay(Duration.ofSeconds(5)).subscribe(v -> subscribeToCandlesticks());
        } catch (Exception e) {
            logger.error("Subscription error: {}", e.getMessage(), e);
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
            JsonNode candle = dataArray.get(0);
            if (!candle.isArray() || candle.size() < 6) {
                logger.warn("Invalid candlestick format for {}: {}", instId, candle);
                return;
            }

            String timestamp = candle.get(0).asText();
            Map<String, Object> candleData = new HashMap<>();
            double open = candle.get(1).asDouble();
            double high = candle.get(2).asDouble();
            double low = candle.get(3).asDouble();
            double close = candle.get(4).asDouble();
            double volume = candle.get(5).asDouble();
            candleData.put("open", open);
            candleData.put("high", high);
            candleData.put("low", low);
            candleData.put("close", close);
            candleData.put("volume", volume);
            candleData.put("price", close);
            candleData.put("timestamp", Long.parseLong(timestamp));

            // Fetch 24h change and volume from TICKER:{PAIR}
            String tickerJson = redisTemplate.opsForValue().get("TICKER:" + instId);
            double change24h = 0.0;
            double volume24h = 0.0;
            if (tickerJson != null) {
                JsonNode ticker = mapper.readTree(tickerJson);
                double lastPrice = ticker.get("last").asDouble();
                double open24h = ticker.get("open24h").asDouble();
                change24h = open24h != 0 ? ((lastPrice - open24h) / open24h) * 100 : 0.0;
                volume24h = ticker.get("vol24h").asDouble();
            }
            candleData.put("change24h", String.format("%.2f", change24h));
            candleData.put("volume24h", volume24h);

            // Fetch logo and circulating supply
            String symbol = instId.replace("-USDT", "");
            Object logoObj = redisTemplate.opsForHash().get("COININFO:" + symbol, "logo");
            Object circulatingSupplyObj = redisTemplate.opsForHash().get("COININFO:" + symbol, "circulating_supply");
            String logo = logoObj != null ? logoObj.toString() : "https://default.logo.png";
            String circulatingSupply = circulatingSupplyObj != null ? circulatingSupplyObj.toString() : "0";
            double circSupply;
            try {
                circSupply = Double.parseDouble(circulatingSupply);
            } catch (NumberFormatException e) {
                logger.warn("Invalid circulating_supply for {}: {}", symbol, circulatingSupply);
                circSupply = 0.0;
            }
            candleData.put("logo", logo);
            candleData.put("circulatingSupply", circSupply);

            String candleJson = mapper.writeValueAsString(candleData);
            logger.debug("Storing candlestick for {}: {}", instId, candleJson);
            redisTemplate.opsForHash().put("CANDLES:" + instId + ":1m", timestamp, candleJson);
            redisTemplate.expire("CANDLES:" + instId + ":1m", 1, TimeUnit.HOURS);

            // Store latest candlestick
            redisTemplate.opsForValue().set("candle:latest:" + instId, candleJson, 1, TimeUnit.HOURS);
            logger.debug("Stored latest candlestick for {}: {}", instId, candleJson);

            redisTemplate.opsForSet().add("active:pairs", instId);
            logger.debug("Stored 1m candlestick for {} at {}", instId, timestamp);
        } catch (JsonProcessingException e) {
            logger.error("JSON parsing error for candlestick: {}", e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Error processing candlestick: {}", e.getMessage(), e);
        }
    }

    private List<String> getValidOkxSpotPairs() {
        try {
            String url = "https://www.okx.com/api/v5/market/tickers?instType=SPOT";
            String response = restTemplate.getForObject(url, String.class);
            if (response == null) {
                logger.warn("No response for OKX spot pairs");
                return Arrays.asList("BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "BNB-USDT", "ADA-USDT");
            }
            logger.debug("OKX spot pairs response: {}", response);

            JsonNode root = mapper.readTree(response);
            JsonNode dataArray = root.get("data");
            if (dataArray == null || !dataArray.isArray()) {
                logger.warn("Invalid OKX spot pairs response: {}", response);
                return Arrays.asList("BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "BNB-USDT", "ADA-USDT");
            }

            List<String> pairs = new ArrayList<>();
            for (JsonNode ticker : dataArray) {
                String instId = ticker.get("instId").asText();
                if (instId.endsWith("-USDT")) {
                    pairs.add(instId);
                }
            }
            logger.info("Fetched {} valid OKX spot pairs: {}", pairs.size(), pairs);
            return pairs;
        } catch (JsonProcessingException e) {
            logger.error("JSON parsing error for OKX pairs: {}", e.getMessage(), e);
            return Arrays.asList("BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "BNB-USDT", "ADA-USDT");
        } catch (Exception e) {
            logger.error("Error fetching OKX pairs: {}", e.getMessage(), e);
            return Arrays.asList("BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "BNB-USDT", "ADA-USDT");
        }
    }

    private List<String> getTopPairsByVolume(List<String> validPairs, int limit) {
        try {
            String url = "https://www.okx.com/api/v5/market/tickers?instType=SPOT";
            String response = restTemplate.getForObject(url, String.class);
            if (response == null) {
                logger.warn("No response for OKX tickers");
                return validPairs.stream().limit(limit).collect(Collectors.toList());
            }
            logger.debug("OKX tickers response: {}", response);

            JsonNode root = mapper.readTree(response);
            JsonNode dataArray = root.get("data");
            if (dataArray == null || !dataArray.isArray()) {
                logger.warn("Invalid OKX tickers response: {}", response);
                return validPairs.stream().limit(limit).collect(Collectors.toList());
            }

            List<Map<String, Object>> tickers = new ArrayList<>();
            for (JsonNode ticker : dataArray) {
                String instId = ticker.get("instId").asText();
                if (validPairs.contains(instId)) {
                    Map<String, Object> tickerData = new HashMap<>();
                    tickerData.put("instId", instId);
                    tickerData.put("vol24h", ticker.get("vol24h").asDouble());
                    tickers.add(tickerData);
                }
            }
            tickers.sort((a, b) -> Double.compare((Double) b.get("vol24h"), (Double) a.get("vol24h")));
            List<String> topPairs = tickers.stream()
                    .map(t -> (String) t.get("instId"))
                    .collect(Collectors.toList());

            // Ensure ETH-USDT and SOL-USDT are included
            List<String> priorityPairs = Arrays.asList("ETH-USDT", "SOL-USDT");
            topPairs.removeAll(priorityPairs);
            topPairs.addAll(0, priorityPairs);
            topPairs = topPairs.stream().limit(limit).collect(Collectors.toList());

            logger.info("Selected top {} pairs: {}", topPairs.size(), topPairs);
            return topPairs;
        } catch (JsonProcessingException e) {
            logger.error("JSON parsing error for top pairs: {}", e.getMessage(), e);
            return validPairs.stream().limit(limit).collect(Collectors.toList());
        } catch (Exception e) {
            logger.error("Error fetching top pairs by volume: {}", e.getMessage(), e);
            return validPairs.stream().limit(limit).collect(Collectors.toList());
        }
    }
}