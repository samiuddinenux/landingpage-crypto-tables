package com.example.landingPage.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.http.*;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;

@Service
public class CryptoFetcherService {

    private final RestTemplate restTemplate = new RestTemplate();
    private final SimpMessagingTemplate messagingTemplate;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper mapper = new ObjectMapper();

    @Value("${coinmarketcap.api.key}")
    private String apiKey;

    @Value("${coinmarketcap.api.url}")
    private String apiUrl;

    public CryptoFetcherService(SimpMessagingTemplate messagingTemplate, StringRedisTemplate redisTemplate) {
        this.messagingTemplate = messagingTemplate;
        this.redisTemplate = redisTemplate;
    }

    @Scheduled(fixedRate = 3600000) // Every hour
    public void fetchAndBroadcastCryptoData() {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-CMC_PRO_API_KEY", apiKey);
        HttpEntity<String> entity = new HttpEntity<>(headers);

        ResponseEntity<String> response = restTemplate.exchange(apiUrl + "?limit=100", HttpMethod.GET, entity, String.class);

        if (response.getStatusCode().is2xxSuccessful()) {
            String data = response.getBody();

            // Save to Redis
            ValueOperations<String, String> ops = redisTemplate.opsForValue();
            ops.set("crypto:latest", data);

            // Extract coin IDs, symbols, market cap, and circulating supply
            try {
                JsonNode root = mapper.readTree(data);
                JsonNode dataArray = root.get("data");

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
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

            // Broadcast original CMC data
            messagingTemplate.convertAndSend("/topic/crypto", data);
        }
    }
}