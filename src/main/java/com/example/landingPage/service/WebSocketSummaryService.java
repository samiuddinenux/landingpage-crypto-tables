package com.example.landingPage.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
public class WebSocketSummaryService extends TextWebSocketHandler {

    private static final Logger logger = LoggerFactory.getLogger(WebSocketSummaryService.class);

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Set<WebSocketSession> sessions = ConcurrentHashMap.newKeySet();

    public WebSocketSummaryService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        sessions.add(session);
        logger.info("New WebSocket connection established: {}", session.getId());
        // Send all data immediately upon connection
        sendSummaryToSession(session);
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
        sessions.remove(session);
        logger.info("WebSocket connection closed: {}, status: {}", session.getId(), status);
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
        logger.error("WebSocket transport error: {}, session: {}", exception.getMessage(), session.getId());
        sessions.remove(session);
        session.close(CloseStatus.SERVER_ERROR);
    }

    @Scheduled(fixedRate = 5000)
    public void broadcastSummary() {
        if (sessions.isEmpty()) {
            logger.debug("No active WebSocket sessions to broadcast to");
            return;
        }

        try {
            String summaryJson = buildSummaryJson();
            TextMessage message = new TextMessage(summaryJson);
            for (WebSocketSession session : sessions) {
                if (session.isOpen()) {
                    try {
                        session.sendMessage(message);
                        logger.debug("Sent summary to session: {}", session.getId());
                    } catch (IOException e) {
                        logger.error("Error sending message to session {}: {}", session.getId(), e.getMessage());
                        sessions.remove(session);
                        try {
                            session.close(CloseStatus.SERVER_ERROR);
                        } catch (IOException ex) {
                            logger.error("Error closing session {}: {}", session.getId(), ex.getMessage());
                        }
                    }
                }
            }
            logger.debug("Broadcasted summary to {} sessions", sessions.size());
        } catch (Exception e) {
            logger.error("Error broadcasting summary: {}", e.getMessage());
        }
    }

    private void sendSummaryToSession(WebSocketSession session) {
        try {
            String summaryJson = buildSummaryJson();
            session.sendMessage(new TextMessage(summaryJson));
            logger.info("Sent initial summary to session: {}", session.getId());
        } catch (IOException e) {
            logger.error("Error sending initial summary to session {}: {}", session.getId(), e.getMessage());
            sessions.remove(session);
            try {
                session.close(CloseStatus.SERVER_ERROR);
            } catch (IOException ex) {
                logger.error("Error closing session {}: {}", ex.getMessage());
            }
        }
    }

    private String buildSummaryJson() throws IOException {
        Map<String, Object> summary = new HashMap<>();

        // Popular coins
        List<Map<String, Object>> popular = new ArrayList<>();
        List<String> priorityPairs = Arrays.asList("BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "BNB-USDT", "ADA-USDT");
        for (String pair : priorityPairs) {
            String json = redisTemplate.opsForValue().get("popular:" + pair);
            if (json != null) {
                Map<String, Object> data = mapper.readValue(json, new TypeReference<Map<String, Object>>() {});
                if (data.containsKey("pair") && data.containsKey("price") &&
                        data.containsKey("change24h") && data.containsKey("timestamp") &&
                        data.containsKey("volume24h") && data.containsKey("logo")) {
                    popular.add(data);
                } else {
                    logger.warn("Incomplete popular data for {}: {}", pair, json);
                    Map<String, Object> fallback = getFallbackPopularData(pair);
                    if (fallback != null) {
                        popular.add(fallback);
                    }
                }
            } else {
                logger.warn("No popular data for {}, using fallback", pair);
                Map<String, Object> fallback = getFallbackPopularData(pair);
                if (fallback != null) {
                    popular.add(fallback);
                }
            }
        }

        if (popular.isEmpty()) {
            logger.warn("Popular section is empty, using defaults");
            popular = getDefaultPopularCoins();
        }

        // Sort popular coins by priority
        popular.sort((a, b) -> {
            String pairA = (String) a.get("pair");
            String pairB = (String) b.get("pair");
            int indexA = priorityPairs.indexOf(pairA);
            int indexB = priorityPairs.indexOf(pairB);
            indexA = indexA == -1 ? Integer.MAX_VALUE : indexA;
            indexB = indexB == -1 ? Integer.MAX_VALUE : indexB;
            return Integer.compare(indexA, indexB);
        });
        popular = popular.stream().limit(6).collect(Collectors.toList());
        summary.put("popular", popular);

        // Candlesticks
        List<Map<String, Object>> candlesticks = new ArrayList<>();
        Set<String> activePairs = redisTemplate.opsForSet().members("active:pairs");
        if (activePairs != null) {
            for (String pair : activePairs) {
                String json = redisTemplate.opsForValue().get("candle:latest:" + pair);
                if (json != null) {
                    Map<String, Object> data = mapper.readValue(json, new TypeReference<Map<String, Object>>() {});
                    candlesticks.add(data);
                }
            }
        }
        summary.put("candlesticks", candlesticks);

        // Gainers
        List<Map<String, Object>> gainers = Collections.emptyList();
        String gainersJson = redisTemplate.opsForValue().get("gainers:json");
        if (gainersJson != null) {
            gainers = mapper.readValue(gainersJson, new TypeReference<List<Map<String, Object>>>(){});
        }
        summary.put("gainers", gainers);

        // Losers
        List<Map<String, Object>> losers = Collections.emptyList();
        String losersJson = redisTemplate.opsForValue().get("losers:json");
        if (losersJson != null) {
            losers = mapper.readValue(losersJson, new TypeReference<List<Map<String, Object>>>(){});
        }
        summary.put("losers", losers);

        return mapper.writeValueAsString(summary);
    }

    private Map<String, Object> getFallbackPopularData(String pair) {
        try {
            String tickerJson = redisTemplate.opsForValue().get("TICKER:" + pair);
            if (tickerJson == null) return null;

            JsonNode ticker = mapper.readTree(tickerJson);
            String symbol = pair.replace("-USDT", "");
            double price = ticker.get("last").asDouble();
            double open24h = ticker.get("open24h").asDouble();
            double change24h = open24h != 0 ? ((price - open24h) / open24h) * 100 : 0.0;
            double volume24h = ticker.get("vol24h").asDouble();
            long timestamp = ticker.has("ts") ? ticker.get("ts").asLong() : System.currentTimeMillis();

            Object logoObj = redisTemplate.opsForHash().get("COININFO:" + symbol, "logo");
            Object circulatingSupplyObj = redisTemplate.opsForHash().get("COININFO:" + symbol, "circulating_supply");
            if (logoObj == null || circulatingSupplyObj == null) {
                logger.debug("Skipping {}: missing logo or circulating_supply", pair);
                return null;
            }

            String logo = logoObj.toString();
            String circulatingSupply = circulatingSupplyObj.toString();
            Double circulatingSupplyValue;
            try {
                circulatingSupplyValue = Double.parseDouble(circulatingSupply);
            } catch (NumberFormatException e) {
                logger.warn("Invalid circulating_supply for {}: {}", symbol, circulatingSupply);
                return null;
            }

            Map<String, Object> data = new HashMap<>();
            data.put("pair", pair);
            data.put("price", price);
            data.put("change24h", String.format("%.2f", change24h)); // Store as String
            data.put("logo", logo);
            data.put("circulatingSupply", circulatingSupplyValue);
            data.put("timestamp", timestamp);
            data.put("volume24h", volume24h);
            return data;
        } catch (Exception e) {
            logger.error("Failed to create fallback data for {}: {}", pair, e.getMessage());
            return null;
        }
    }

    private List<Map<String, Object>> getDefaultPopularCoins() {
        List<Map<String, Object>> defaults = new ArrayList<>();
        String[] pairs = {"BTC-USDT", "ETH-USDT", "SOL-USDT"};
        for (String pair : pairs) {
            Map<String, Object> data = getFallbackPopularData(pair);
            if (data != null) {
                defaults.add(data);
            }
        }
        return defaults;
    }
}