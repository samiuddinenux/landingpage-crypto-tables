package com.example.landingPage.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.*;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import com.example.landingPage.config.WebSocketConfig.WebSocketHandler;

import java.util.*;

@Service
public class CryptoFetcherService {

    private static final Logger logger = LoggerFactory.getLogger(CryptoFetcherService.class);

    private final RestTemplate restTemplate = new RestTemplate();
    private final WebSocketHandler webSocketHandler;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper mapper = new ObjectMapper();

    @Value("${coinmarketcap.api.key}")
    private String apiKey;

    @Value("${coinmarketcap.api.url}")
    private String apiUrl;

    public CryptoFetcherService(WebSocketHandler webSocketHandler, StringRedisTemplate redisTemplate) {
        this.webSocketHandler = webSocketHandler;
        this.redisTemplate = redisTemplate;
    }

    @Scheduled(fixedRate = 3600000) // Every hour
    public void fetchAndBroadcastCryptoData() {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.set("X-CMC_PRO_API_KEY", apiKey);
            HttpEntity<String> entity = new HttpEntity<>(headers);

            ResponseEntity<String> response = restTemplate.exchange(apiUrl + "?limit=100", HttpMethod.GET, entity, String.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                String data = response.getBody();
                redisTemplate.opsForValue().set("crypto:latest", data);
                logger.info("Stored crypto:latest in Redis");

                // Extract symbols, market cap, and circulating supply
                JsonNode root = mapper.readTree(data);
                JsonNode dataArray = root.get("data");
                if (dataArray == null || !dataArray.isArray()) {
                    logger.warn("Invalid CMC data structure");
                    return;
                }

                List<String> ids = new ArrayList<>();
                for (JsonNode coin : dataArray) {
                    ids.add(coin.get("id").asText());
                    String symbol = coin.get("symbol").asText();
                    double marketCap = coin.get("quote").get("USD").get("market_cap").asDouble();
                    double circulatingSupply = coin.get("circulating_supply").asDouble();
                    redisTemplate.opsForValue().set("marketcap:" + symbol, String.format("%.2f", marketCap));
                    redisTemplate.opsForValue().set("supply:" + symbol, String.format("%.2f", circulatingSupply));
                }

                // Fetch logo info
                String idsParam = String.join(",", ids);
                String infoUrl = "https://pro-api.coinmarketcap.com/v2/cryptocurrency/info?id=" + idsParam;
                ResponseEntity<String> infoResponse = restTemplate.exchange(infoUrl, HttpMethod.GET, entity, String.class);

                if (infoResponse.getStatusCode().is2xxSuccessful()) {
                    JsonNode infoData = mapper.readTree(infoResponse.getBody()).get("data");
                    for (String id : ids) {
                        JsonNode coinInfo = infoData.get(id);
                        if (coinInfo != null && coinInfo.has("symbol") && coinInfo.has("logo")) {
                            String symbol = coinInfo.get("symbol").asText();
                            String logo = coinInfo.get("logo").asText();
                            redisTemplate.opsForValue().set("logo:" + symbol, logo);
                        }
                    }
                    logger.info("Stored logos for {} coins", ids.size());
                } else {
                    logger.warn("Failed to fetch CMC info: {}", infoResponse.getStatusCode());
                }

                // Broadcast the raw JSON data to WebSocket clients
                webSocketHandler.broadcast(data);
                logger.info("Broadcasted crypto data to WebSocket clients");
            } else {
                logger.error("CMC API request failed: {}", response.getStatusCode());
            }
        } catch (Exception e) {
            logger.error("Error fetching CMC data: {}", e.getMessage());
        }
    }
}