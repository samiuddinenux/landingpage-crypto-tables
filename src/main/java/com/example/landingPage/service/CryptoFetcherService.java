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
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import com.example.landingPage.config.WebSocketConfig.WebSocketHandler;
import reactor.core.publisher.Mono;

import java.time.Duration;
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

    @Scheduled(fixedRate = 1800000) // Every 30 minutes
    public void fetchAndBroadcastCryptoData() {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.set("X-CMC_PRO_API_KEY", apiKey);
            HttpEntity<String> entity = new HttpEntity<>(headers);

            // Fetch listings
            String listingsUrl = apiUrl + "?limit=200";
            ResponseEntity<String> response = restTemplate.exchange(listingsUrl, HttpMethod.GET, entity, String.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                String data = response.getBody();
                redisTemplate.opsForValue().set("crypto:latest", data);
                logger.info("Stored crypto:latest in Redis ({} bytes)", data.length());

                // Extract symbols, market cap, and circulating supply
                JsonNode root = mapper.readTree(data);
                JsonNode dataArray = root.get("data");
                if (dataArray == null || !dataArray.isArray()) {
                    logger.warn("Invalid CMC data structure");
                    return;
                }

                List<String> ids = new ArrayList<>();
                for (JsonNode coin : dataArray) {
                    String id = coin.get("id").asText();
                    String symbol = coin.get("symbol").asText();
                    JsonNode usdQuote = coin.get("quote").get("USD");
                    double marketCap = usdQuote.has("market_cap") ? usdQuote.get("market_cap").asDouble() : 0.0;
                    double circulatingSupply = coin.has("circulating_supply") ? coin.get("circulating_supply").asDouble() : 0.0;
                    ids.add(id);
                    redisTemplate.opsForValue().set("marketcap:" + symbol, String.format("%.2f", marketCap));
                    redisTemplate.opsForValue().set("supply:" + symbol, String.format("%.2f", circulatingSupply));
                }

                // Fetch logo info
                if (!ids.isEmpty()) {
                    String idsParam = String.join(",", ids);
                    String infoUrl = "https://pro-api.coinmarketcap.com/v2/cryptocurrency/info?id=" + idsParam;
                    ResponseEntity<String> infoResponse = restTemplate.exchange(infoUrl, HttpMethod.GET, entity, String.class);

                    if (infoResponse.getStatusCode().is2xxSuccessful()) {
                        JsonNode infoData = mapper.readTree(infoResponse.getBody()).get("data");
                        int logoCount = 0;
                        for (String id : ids) {
                            JsonNode coinInfo = infoData.get(id);
                            if (coinInfo != null && coinInfo.has("symbol") && coinInfo.has("logo")) {
                                String symbol = coinInfo.get("symbol").asText();
                                String logo = coinInfo.get("logo").asText();
                                redisTemplate.opsForValue().set("logo:" + symbol, logo);
                                logoCount++;
                            }
                            else {
                                logger.warn("Logo not found for coin ID: {}", id);

                            }
                        }
                        logger.info("Stored logos for {} of {} coins", logoCount, ids.size());
                    } else {
                        logger.warn("Failed to fetch CMC info: {}", infoResponse.getStatusCode());
                    }
                } else {
                    logger.warn("No coin IDs fetched from listings");
                }

                // Broadcast raw JSON to WebSocket clients
                webSocketHandler.broadcast(data);
                logger.info("Broadcasted crypto data to WebSocket clients");
            } else {
                logger.error("CMC API listings request failed: {}", response.getStatusCode());
                retryFetch();
            }
        } catch (RestClientException e) {
            logger.error("Network error fetching CMC data: {}", e.getMessage());
            retryFetch();
        } catch (Exception e) {
            logger.error("Error processing CMC data: {}", e.getMessage());
            retryFetch();
        }
    }

    private void retryFetch() {
        logger.info("Retrying CMC fetch in 60s");
        Mono.delay(Duration.ofSeconds(60)).subscribe(v -> fetchAndBroadcastCryptoData());
    }
}