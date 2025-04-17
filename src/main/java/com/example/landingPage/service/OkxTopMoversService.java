package com.example.landingPage.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
public class OkxTopMoversService {

    private static final Logger logger = LoggerFactory.getLogger(OkxTopMoversService.class);

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper mapper = new ObjectMapper();

    public OkxTopMoversService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Scheduled(fixedRate = 5000)
    public void updateTopMovers() {
        try {
            List<Map<String, Object>> gainers = new ArrayList<>();
            List<Map<String, Object>> losers = new ArrayList<>();

            // Scan ACTPAIR:ACTIVE
            Set<String> activePairs = redisTemplate.opsForSet().members("active:pairs");
            if (activePairs == null || activePairs.isEmpty()) {
                logger.warn("No active pairs found in active:pairs");
                updateFromTickers(gainers, losers);
                finalizeTopMovers(gainers, losers);
                return;
            }

            for (String instId : activePairs) {
                String candleJson = redisTemplate.opsForValue().get("candle:latest:" + instId);
                if (candleJson == null) continue;

                JsonNode candleData;
                try {
                    candleData = mapper.readTree(candleJson);
                } catch (JsonProcessingException e) {
                    logger.warn("Failed to parse candle data for {}: {}", instId, e.getMessage());
                    continue;
                }

                if (!hasRequiredFields(candleData)) {
                    logger.debug("Skipping {}: missing required fields", instId);
                    continue;
                }

                double change = candleData.get("change24h").asDouble();
                double price = candleData.get("price").asDouble();
                long timestamp = candleData.get("timestamp").asLong();
                double volume24h = candleData.get("volume24h").asDouble();
                String logo = candleData.get("logo").asText();
                double circulatingSupply = candleData.get("circulatingSupply").asDouble();

                Map<String, Object> entry = new HashMap<>();
                entry.put("pair", instId);
                entry.put("change24h", change); // Store as Double
                entry.put("price", price);
                entry.put("timestamp", timestamp);
                entry.put("volume24h", volume24h);
                entry.put("logo", logo);
                entry.put("circulatingSupply", circulatingSupply);

                if (change > 0) {
                    gainers.add(entry);
                } else if (change < 0) {
                    losers.add(entry);
                }
            }

            finalizeTopMovers(gainers, losers);
        } catch (Exception e) {
            logger.error("Error updating top movers: {}", e.getMessage());
        }
    }

    private boolean hasRequiredFields(JsonNode candleData) {
        return candleData.has("change24h") && !candleData.get("change24h").isNull() &&
                candleData.has("price") && !candleData.get("price").isNull() &&
                candleData.has("timestamp") && !candleData.get("timestamp").isNull() &&
                candleData.has("volume24h") && !candleData.get("volume24h").isNull() &&
                candleData.has("logo") && !candleData.get("logo").isNull() &&
                candleData.has("circulatingSupply") && !candleData.get("circulatingSupply").isNull();
    }

    private void updateFromTickers(List<Map<String, Object>> gainers, List<Map<String, Object>> losers) {
        logger.info("Falling back to ticker data for top movers");
        ScanOptions options = ScanOptions.scanOptions().match("TICKER:*").count(100).build();
        try (var cursor = redisTemplate.scan(options)) {
            while (cursor.hasNext()) {
                String key = cursor.next();
                String instId = key.replace("TICKER:", "");
                String tickerJson = redisTemplate.opsForValue().get(key);
                if (tickerJson == null) continue;

                JsonNode ticker = mapper.readTree(tickerJson);
                double lastPrice = ticker.get("last").asDouble();
                double open24h = ticker.get("open24h").asDouble();
                double vol24h = ticker.get("vol24h").asDouble();
                long timestamp = ticker.has("ts") ? ticker.get("ts").asLong() : System.currentTimeMillis();
                double change24h = open24h != 0 ? ((lastPrice - open24h) / open24h) * 100 : 0.0;

                String symbol = instId.replace("-USDT", "");
                Object logoObj = redisTemplate.opsForHash().get("COININFO:" + symbol, "logo");
                Object circulatingSupplyObj = redisTemplate.opsForHash().get("COININFO:" + symbol, "circulating_supply");
                if (logoObj == null || circulatingSupplyObj == null) {
                    logger.debug("Skipping {}: missing logo or circulating_supply", instId);
                    continue;
                }

                String logo = logoObj.toString();
                String circulatingSupply = circulatingSupplyObj.toString();
                double circSupply;
                try {
                    circSupply = Double.parseDouble(circulatingSupply);
                } catch (NumberFormatException e) {
                    logger.warn("Invalid circulating_supply for {}: {}", symbol, circulatingSupply);
                    continue;
                }

                Map<String, Object> entry = new HashMap<>();
                entry.put("pair", instId);
                entry.put("change24h", change24h); // Store as Double
                entry.put("price", lastPrice);
                entry.put("timestamp", timestamp);
                entry.put("volume24h", vol24h);
                entry.put("logo", logo);
                entry.put("circulatingSupply", circSupply);

                if (change24h > 0) {
                    gainers.add(entry);
                } else if (change24h < 0) {
                    losers.add(entry);
                }
            }
        } catch (Exception e) {
            logger.error("Error processing tickers for top movers: {}", e.getMessage());
        }
    }

    private void finalizeTopMovers(List<Map<String, Object>> gainers, List<Map<String, Object>> losers) {
        try {
            // Sort gainers and losers by change24h (Double)
            gainers.sort((a, b) -> Double.compare((Double) b.get("change24h"), (Double) a.get("change24h")));
            losers.sort((a, b) -> Double.compare((Double) a.get("change24h"), (Double) b.get("change24h")));

            gainers = gainers.stream().limit(6).collect(Collectors.toList());
            losers = losers.stream().limit(6).collect(Collectors.toList());

            String gainersJson = mapper.writeValueAsString(gainers);
            String losersJson = mapper.writeValueAsString(losers);
            redisTemplate.opsForValue().set("gainers:json", gainersJson, 1, TimeUnit.HOURS);
            redisTemplate.opsForValue().set("losers:json", losersJson, 1, TimeUnit.HOURS);
            logger.info("Updated top movers: gainers={}, losers={}", gainers.size(), losers.size());
        } catch (JsonProcessingException e) {
            logger.error("Error serializing top movers: {}", e.getMessage());
        }
    }
}