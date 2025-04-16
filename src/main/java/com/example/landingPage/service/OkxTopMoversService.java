package com.example.landingPage.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;
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

            logger.debug("Scanning candle:latest:* for top movers");
            Set<String> candleKeys = redisTemplate.keys("candle:latest:*");
            if (candleKeys == null || candleKeys.isEmpty()) {
                logger.warn("No candle:latest:* keys found");
                updateFromTickers(gainers, losers);
                finalizeTopMovers(gainers, losers);
                return;
            }
            logger.debug("Found {} candle:latest:* keys", candleKeys.size());

            // Fetch volume data
            Map<String, Double> volumeMap = new HashMap<>();
            String tickersJson = redisTemplate.opsForValue().get("tickers:latest");
            if (tickersJson != null) {
                try {
                    JsonNode tickers = mapper.readTree(tickersJson);
                    for (JsonNode ticker : tickers) {
                        String instId = ticker.get("instId").asText();
                        double vol24h = ticker.has("vol24h") ? ticker.get("vol24h").asDouble() : 0.0;
                        // Cap volume to prevent overflow
                        if (vol24h > 1E12) {
                            logger.warn("Capping inflated volume for {}: {}", instId, vol24h);
                            vol24h = 1E12;
                        }
                        volumeMap.put(instId, vol24h);
                    }
                    logger.debug("Loaded volume data for {} pairs", volumeMap.size());
                } catch (JsonProcessingException e) {
                    logger.warn("Failed to parse tickers:latest: {}", e.getMessage());
                }
            } else {
                logger.warn("No tickers:latest found in Redis");
            }

            for (String key : candleKeys) {
                String instId = key.replace("candle:latest:", "");
                String candleJson = redisTemplate.opsForValue().get(key);
                if (candleJson == null) {
                    logger.debug("No data for key {}", key);
                    continue;
                }

                JsonNode candleData;
                try {
                    candleData = mapper.readTree(candleJson);
                } catch (JsonProcessingException e) {
                    logger.warn("Failed to parse candle data for {}: {}", instId, e.getMessage());
                    continue;
                }

                if (!candleData.has("change24h") || !candleData.has("price") || !candleData.has("timestamp")) {
                    logger.warn("Missing required fields for {}: {}", instId, candleData);
                    continue;
                }

                double change;
                double price;
                long timestamp;
                try {
                    change = candleData.get("change24h").asDouble();
                    price = candleData.get("price").asDouble();
                    timestamp = candleData.get("timestamp").asLong();
                } catch (Exception e) {
                    logger.warn("Invalid field types for {}: {}", instId, e.getMessage());
                    continue;
                }

                logger.debug("Processing {}: change24h={}, price={}", instId, change, price);

                String symbol = instId.replace("-USDT", "");
                String logo = redisTemplate.opsForValue().get("logo:" + symbol);
                String circulatingSupply = redisTemplate.opsForValue().get("supply:" + symbol);

                Map<String, Object> entry = new HashMap<>();
                entry.put("pair", instId);
                entry.put("change24h", change);
                entry.put("price", price);
                entry.put("timestamp", timestamp);
                entry.put("volume24h", volumeMap.getOrDefault(instId, 0.0));

                // Only include logo and circulatingSupply if available
                if (logo != null && !logo.isEmpty()) {
                    entry.put("logo", logo);
                }
                if (circulatingSupply != null && !circulatingSupply.isEmpty()) {
                    try {
                        entry.put("circulatingSupply", Double.parseDouble(circulatingSupply));
                    } catch (NumberFormatException e) {
                        logger.warn("Invalid circulatingSupply for {}: {}", symbol, circulatingSupply);
                    }
                }

                if (change > 0) {
                    gainers.add(entry);
                    logger.debug("Added {} to gainers: change24h={}", instId, change);
                } else if (change < 0) {
                    losers.add(entry);
                    logger.debug("Added {} to losers: change24h={}", instId, change);
                } else {
                    logger.debug("Skipped {}: change24h=0", instId);
                }
            }

            finalizeTopMovers(gainers, losers);

        } catch (Exception e) {
            logger.error("Error updating top movers: {}", e.getMessage());
        }
    }

    private void updateFromTickers(List<Map<String, Object>> gainers, List<Map<String, Object>> losers) {
        logger.info("Falling back to ticker data for top movers");
        String tickersJson = redisTemplate.opsForValue().get("tickers:latest");
        if (tickersJson == null) {
            logger.warn("No tickers:latest available for fallback");
            // Default to major coins
            addDefaultCoins(gainers, losers);
            return;
        }

        try {
            JsonNode tickers = mapper.readTree(tickersJson);
            for (JsonNode ticker : tickers) {
                String instId = ticker.get("instId").asText();
                if (!instId.endsWith("-USDT")) continue;

                double lastPrice = ticker.has("last") ? ticker.get("last").asDouble() : 0.0;
                double open24h = ticker.has("open24h") ? ticker.get("open24h").asDouble() : 0.0;
                double vol24h = ticker.has("vol24h") ? ticker.get("vol24h").asDouble() : 0.0;
                if (vol24h > 1E12) {
                    logger.warn("Capping inflated volume for {}: {}", instId, vol24h);
                    vol24h = 1E12;
                }
                long timestamp = ticker.has("ts") ? ticker.get("ts").asLong() : System.currentTimeMillis();

                double change24h = open24h != 0 ? ((lastPrice - open24h) / open24h) * 100 : 0.0;

                String symbol = instId.replace("-USDT", "");
                String logo = redisTemplate.opsForValue().get("logo:" + symbol);
                String circulatingSupply = redisTemplate.opsForValue().get("supply:" + symbol);

                Map<String, Object> entry = new HashMap<>();
                entry.put("pair", instId);
                entry.put("change24h", change24h);
                entry.put("price", lastPrice);
                entry.put("timestamp", timestamp);
                entry.put("volume24h", vol24h);

                if (logo != null && !logo.isEmpty()) {
                    entry.put("logo", logo);
                }
                if (circulatingSupply != null && !circulatingSupply.isEmpty()) {
                    try {
                        entry.put("circulatingSupply", Double.parseDouble(circulatingSupply));
                    } catch (NumberFormatException e) {
                        logger.warn("Invalid circulatingSupply for {}: {}", symbol, circulatingSupply);
                    }
                }

                if (change24h > 0) {
                    gainers.add(entry);
                    logger.debug("Fallback: Added {} to gainers: change24h={}", instId, change24h);
                } else if (change24h < 0) {
                    losers.add(entry);
                    logger.debug("Fallback: Added {} to losers: change24h={}", instId, change24h);
                }
            }
        } catch (Exception e) {
            logger.error("Error processing ticker fallback: {}", e.getMessage());
            addDefaultCoins(gainers, losers);
        }
    }

    private void addDefaultCoins(List<Map<String, Object>> gainers, List<Map<String, Object>> losers) {
        logger.info("Using default coins as final fallback");
        List<String> defaults = Arrays.asList("BTC-USDT", "ETH-USDT", "SOL-USDT");
        long timestamp = System.currentTimeMillis();

        for (String instId : defaults) {
            String candleJson = redisTemplate.opsForValue().get("candle:latest:" + instId);
            if (candleJson == null) continue;

            try {
                JsonNode candleData = mapper.readTree(candleJson);
                if (!candleData.has("change24h") || !candleData.has("price")) continue;

                double change = candleData.get("change24h").asDouble();
                double price = candleData.get("price").asDouble();
                double volume24h = candleData.has("volume24h") ? candleData.get("volume24h").asDouble() : 0.0;

                String symbol = instId.replace("-USDT", "");
                String logo = redisTemplate.opsForValue().get("logo:" + symbol);
                String circulatingSupply = redisTemplate.opsForValue().get("supply:" + symbol);

                Map<String, Object> entry = new HashMap<>();
                entry.put("pair", instId);
                entry.put("change24h", change);
                entry.put("price", price);
                entry.put("timestamp", timestamp);
                entry.put("volume24h", volume24h);

                if (logo != null && !logo.isEmpty()) {
                    entry.put("logo", logo);
                }
                if (circulatingSupply != null && !circulatingSupply.isEmpty()) {
                    try {
                        entry.put("circulatingSupply", Double.parseDouble(circulatingSupply));
                    } catch (NumberFormatException e) {
                        logger.warn("Invalid circulatingSupply for {}: {}", symbol, circulatingSupply);
                    }
                }

                if (change > 0) {
                    gainers.add(entry);
                    logger.debug("Default: Added {} to gainers: change24h={}", instId, change);
                } else if (change < 0) {
                    losers.add(entry);
                    logger.debug("Default: Added {} to losers: change24h={}", instId, change);
                }
            } catch (Exception e) {
                logger.warn("Failed to process default coin {}: {}", instId, e.getMessage());
            }
        }
    }

    private void finalizeTopMovers(List<Map<String, Object>> gainers, List<Map<String, Object>> losers) {
        try {
            // Sort by change24h, then volume
            gainers.sort((a, b) -> {
                int changeCompare = Double.compare((Double) b.get("change24h"), (Double) a.get("change24h"));
                if (changeCompare != 0) return changeCompare;
                return Double.compare((Double) b.get("volume24h"), (Double) a.get("volume24h"));
            });
            losers.sort((a, b) -> {
                int changeCompare = Double.compare((Double) a.get("change24h"), (Double) b.get("change24h"));
                if (changeCompare != 0) return changeCompare;
                return Double.compare((Double) b.get("volume24h"), (Double) a.get("volume24h"));
            });

            // Limit to 6 entries
            gainers = gainers.stream().limit(6).collect(Collectors.toList());
            losers = losers.stream().limit(6).collect(Collectors.toList());

            // Log if lists are empty
            if (gainers.isEmpty()) {
                logger.warn("Gainers list is empty after processing");
            }
            if (losers.isEmpty()) {
                logger.warn("Losers list is empty after processing");
            }

            // Store in Redis
            String gainersJson = mapper.writeValueAsString(gainers);
            String losersJson = mapper.writeValueAsString(losers);
            redisTemplate.opsForValue().set("gainers:json", gainersJson);
            redisTemplate.opsForValue().set("losers:json", losersJson);

            logger.info("Updated top movers: gainers={}, losers={}", gainers.size(), losers.size());

        } catch (JsonProcessingException e) {
            logger.error("Error serializing top movers: {}", e.getMessage());
        }
    }
}