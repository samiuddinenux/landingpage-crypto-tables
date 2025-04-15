package com.example.landingPage.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class WebSocketSummaryService {

    private static final Logger logger = LoggerFactory.getLogger(WebSocketSummaryService.class);

    private final SimpMessagingTemplate messagingTemplate;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    public WebSocketSummaryService(SimpMessagingTemplate messagingTemplate, StringRedisTemplate redisTemplate) {
        this.messagingTemplate = messagingTemplate;
        this.redisTemplate = redisTemplate;
        this.redisTemplate.afterPropertiesSet();
    }

    @Scheduled(fixedRate = 1000)
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

            messagingTemplate.convertAndSend("/topic/summary", summary);
            logger.debug("Broadcasted summary: popular={}, candlesticks={}, gainers={}, losers={}",
                    popular.size(), candlesticks.size(), gainers.size(), losers.size());

        } catch (Exception e) {
            logger.error("Error broadcasting summary: {}", e.getMessage());
        }
    }
}