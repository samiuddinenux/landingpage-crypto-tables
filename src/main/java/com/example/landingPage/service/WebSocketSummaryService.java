package com.example.landingPage.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import com.example.landingPage.config.WebSocketConfig.WebSocketHandler;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class WebSocketSummaryService {

    private static final Logger logger = LoggerFactory.getLogger(WebSocketSummaryService.class);

    private final WebSocketHandler webSocketHandler;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    public WebSocketSummaryService(WebSocketHandler webSocketHandler, StringRedisTemplate redisTemplate) {
        this.webSocketHandler = webSocketHandler;
        this.redisTemplate = redisTemplate;
    }

    @Scheduled(fixedRate = 60000) // Update every minute
    public void updateGainersAndLosers() {
        try {
            List<Map<String, Object>> allPairs = new ArrayList<>();

            // Collect data from candle:latest:*
            for (String key : redisTemplate.keys("candle:latest:*")) {
                String json = redisTemplate.opsForValue().get(key);
                if (json != null) {
                    try {
                        Map<String, Object> data = mapper.readValue(json, new TypeReference<Map<String, Object>>() {});
                        if (data.containsKey("pair") && data.containsKey("change24h") && data.containsKey("price")) {
                            allPairs.add(data);
                        }
                    } catch (Exception e) {
                        logger.error("Failed to parse candlestick data for key {}: {}", key, e.getMessage());
                    }
                }
            }

            // Fallback: Use ticker data
            if (allPairs.size() < 12) {
                for (String key : redisTemplate.keys("ticker:*")) {
                    String json = redisTemplate.opsForValue().get(key);
                    if (json != null) {
                        try {
                            JsonNode ticker = mapper.readTree(json);
                            String instId = ticker.get("instId").asText();
                            double lastPrice = ticker.get("last").asDouble();
                            double open24h = ticker.get("open24h").asDouble();
                            double change24h = open24h != 0 ? ((lastPrice - open24h) / open24h) * 100 : 0.0;
                            String symbol = instId.replace("-USDT", "");
                            String logo = redisTemplate.opsForValue().get("logo:" + symbol);
                            String circulatingSupply = redisTemplate.opsForValue().get("supply:" + symbol);
                            double volume24h = ticker.get("vol24h").asDouble();

                            Map<String, Object> data = new HashMap<>();
                            data.put("pair", instId);
                            data.put("price", lastPrice);
                            data.put("change24h", String.format("%.2f", change24h));
                            data.put("logo", logo);
                            data.put("circulatingSupply", circulatingSupply != null ? Double.parseDouble(circulatingSupply) : null);
                            String candleJson = redisTemplate.opsForValue().get("candle:latest:" + instId);
                            long timestamp = System.currentTimeMillis();
                            if (candleJson != null) {
                                JsonNode candleData = mapper.readTree(candleJson);
                                if (candleData.has("timestamp")) {
                                    timestamp = candleData.get("timestamp").asLong();
                                }
                            }
                            data.put("timestamp", timestamp);
                            data.put("volume24h", volume24h);
                            allPairs.add(data);
                        } catch (Exception e) {
                            logger.error("Failed to parse ticker data for key {}: {}", key, e.getMessage());
                        }
                    }
                }
            }

            // Sort by change24h
            allPairs.sort((a, b) -> Double.compare(
                    Double.parseDouble((String) b.get("change24h")),
                    Double.parseDouble((String) a.get("change24h"))
            ));

            // Select top 6 gainers
            List<Map<String, Object>> gainers = allPairs.stream()
                    .filter(p -> Double.parseDouble((String) p.get("change24h")) > 0)
                    .limit(6)
                    .collect(Collectors.toList());

            // Select top 6 losers
            List<Map<String, Object>> losers = allPairs.stream()
                    .filter(p -> Double.parseDouble((String) p.get("change24h")) < 0)
                    .sorted((a, b) -> Double.compare(
                            Double.parseDouble((String) a.get("change24h")),
                            Double.parseDouble((String) b.get("change24h"))
                    ))
                    .limit(6)
                    .collect(Collectors.toList());

            // Store in Redis
            redisTemplate.opsForValue().set("gainers:json", mapper.writeValueAsString(gainers));
            redisTemplate.opsForValue().set("losers:json", mapper.writeValueAsString(losers));
            logger.info("Updated gainers:json ({} pairs), losers:json ({} pairs)", gainers.size(), losers.size());

        } catch (Exception e) {
            logger.error("Error updating gainers and losers: {}", e.getMessage());
        }
    }

    @Scheduled(fixedRate = 5000)
    public void broadcastSummary() {
        try {
            Map<String, Object> summary = new HashMap<>();

            // Popular coins
            List<Map<String, Object>> popular = new ArrayList<>();
            List<String> priorityPairs = Arrays.asList("BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "BNB-USDT", "ADA-USDT");
            Set<String> popularKeys = redisTemplate.keys("popular:*");
            if (popularKeys == null || popularKeys.isEmpty()) {
                logger.warn("No popular:* keys, falling back to candle:latest:*");
                for (String pair : priorityPairs) {
                    String json = redisTemplate.opsForValue().get("candle:latest:" + pair);
                    if (json != null) {
                        try {
                            Map<String, Object> data = mapper.readValue(json, new TypeReference<Map<String, Object>>() {});
                            if (data.containsKey("pair") && data.containsKey("price") &&
                                    data.containsKey("change24h") && data.containsKey("timestamp") &&
                                    data.containsKey("volume24h")) {
                                popular.add(data);
                            }
                        } catch (Exception e) {
                            logger.error("Failed to parse candle data for {}: {}", pair, e.getMessage());
                        }
                    }
                }
            } else {
                for (String key : popularKeys) {
                    String json = redisTemplate.opsForValue().get(key);
                    if (json != null) {
                        try {
                            Map<String, Object> data = mapper.readValue(json, new TypeReference<Map<String, Object>>() {});
                            if (data.containsKey("pair") && data.containsKey("price") &&
                                    data.containsKey("change24h") && data.containsKey("timestamp") &&
                                    data.containsKey("volume24h")) {
                                popular.add(data);
                            } else {
                                logger.warn("Incomplete popular data for key {}: {}", key, json);
                            }
                        } catch (Exception e) {
                            logger.error("Failed to parse popular data for key {}: {}", key, e.getMessage());
                        }
                    }
                }
            }

            // Sort and limit popular
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
            if (popular.isEmpty()) {
                logger.warn("Popular section is empty");
            } else if (popular.size() < 6) {
                logger.warn("Popular section has {} pairs, expected 6", popular.size());
            }
            summary.put("popular", popular);

            // Candlesticks
            List<Map<String, Object>> candlesticks = new ArrayList<>();
            for (String key : redisTemplate.keys("candle:latest:*")) {
                String json = redisTemplate.opsForValue().get(key);
                if (json != null) {
                    try {
                        Map<String, Object> data = mapper.readValue(json, new TypeReference<Map<String, Object>>() {});
                        candlesticks.add(data);
                    } catch (Exception e) {
                        logger.error("Failed to parse candlestick data for key {}: {}", key, e.getMessage());
                    }
                }
            }
            summary.put("candlesticks", candlesticks);

            // Gainers
            List<Map<String, Object>> gainers = Collections.emptyList();
            String gainersJson = redisTemplate.opsForValue().get("gainers:json");
            if (gainersJson != null) {
                try {
                    gainers = mapper.readValue(gainersJson, new TypeReference<List<Map<String, Object>>>() {});
                } catch (Exception e) {
                    logger.error("Failed to parse gainers data: {}", e.getMessage());
                }
            }
            summary.put("gainers", gainers);

            // Losers
            List<Map<String, Object>> losers = Collections.emptyList();
            String losersJson = redisTemplate.opsForValue().get("losers:json");
            if (losersJson != null) {
                try {
                    losers = mapper.readValue(losersJson, new TypeReference<List<Map<String, Object>>>() {});
                } catch (Exception e) {
                    logger.error("Failed to parse losers data: {}", e.getMessage());
                }
            }
            summary.put("losers", losers);

            String summaryJson = mapper.writeValueAsString(summary);
            webSocketHandler.broadcast(summaryJson);
            logger.debug("Broadcasted summary: popular={}, candlesticks={}, gainers={}, losers={}",
                    popular.size(), candlesticks.size(), gainers.size(), losers.size());

        } catch (Exception e) {
            logger.error("Error broadcasting summary: {}", e.getMessage());
        }
    }
}