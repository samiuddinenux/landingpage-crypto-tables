package com.example.landingPage.service;

import com.fasterxml.jackson.core.type.TypeReference;
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

    @Scheduled(fixedRate = 5000)
    public void broadcastSummary() {
        try {
            Map<String, Object> summary = new HashMap<>();

            // Popular coins
            List<Map<String, Object>> popular = new ArrayList<>();
            for (String key : redisTemplate.keys("popular:*")) {
                String json = redisTemplate.opsForValue().get(key);
                if (json != null) {
                    try {
                        Map<String, Object> data = mapper.readValue(json, new TypeReference<Map<String, Object>>() {});
                        popular.add(data);
                    } catch (Exception e) {
                        logger.error("Failed to parse popular data for key {}: {}", key, e.getMessage());
                    }
                }
            }
            popular.sort((a, b) -> {
                String pairA = (String) a.get("pair");
                String pairB = (String) b.get("pair");
                List<String> order = Arrays.asList("BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "BNB-USDT", "ADA-USDT");
                int indexA = order.indexOf(pairA);
                int indexB = order.indexOf(pairB);
                indexA = indexA == -1 ? Integer.MAX_VALUE : indexA;
                indexB = indexB == -1 ? Integer.MAX_VALUE : indexB;
                return Integer.compare(indexA, indexB);
            });
            popular = popular.stream().limit(6).collect(Collectors.toList());
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
            } else {
                logger.warn("No gainers:json found in Redis");
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
            } else {
                logger.warn("No losers:json found in Redis");
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