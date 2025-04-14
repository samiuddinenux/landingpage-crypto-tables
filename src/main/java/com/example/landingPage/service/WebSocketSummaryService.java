package com.example.landingPage.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.MimeTypeUtils;

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
    }

    @Scheduled(fixedRate = 1000)
    public void broadcastSummary() {
        try {
            // Collect popular coins data
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

            // Collect candlestick latest data (includes change24h, price, circulatingSupply)
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

            // Collect top gainers
            List<Map<String, Object>> gainers = Collections.emptyList();
            if (redisTemplate.hasKey("gainers:json")) {
                String json = redisTemplate.opsForValue().get("gainers:json");
                if (json != null) {
                    try {
                        gainers = mapper.readValue(json, new TypeReference<List<Map<String, Object>>>() {});
                    } catch (Exception e) {
                        logger.error("Failed to parse gainers data: {}", e.getMessage());
                    }
                }
            }

            // Collect top losers
            List<Map<String, Object>> losers = Collections.emptyList();
            if (redisTemplate.hasKey("losers:json")) {
                String json = redisTemplate.opsForValue().get("losers:json");
                if (json != null) {
                    try {
                        losers = mapper.readValue(json, new TypeReference<List<Map<String, Object>>>() {});
                    } catch (Exception e) {
                        logger.error("Failed to parse losers data: {}", e.getMessage());
                    }
                }
            }

            // Prepare the summary object
            Map<String, Object> summary = new HashMap<>();
            summary.put("popular", popular);
            summary.put("candlesticks", candlesticks);
            summary.put("gainers", gainers);
            summary.put("losers", losers);

            messagingTemplate.convertAndSend(
                    "/topic/summary",
                    summary,
                    new MessageHeaders(Map.of(
                            MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON
                    ))
            );

        } catch (Exception e) {
            logger.error("Error broadcasting summary: {}", e.getMessage());
        }
    }
}