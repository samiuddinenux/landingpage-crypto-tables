package com.example.landingPage.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.*;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Service
public class CryptoFetcherService {

    private static final Logger logger = LoggerFactory.getLogger(CryptoFetcherService.class);

    private final RestTemplate restTemplate = new RestTemplate();
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper mapper = new ObjectMapper();


    @Value("${coinmarketcap.api.key}")
    private String apiKey;

    @Value("${coinmarketcap.api.url}")
    private String apiUrl;

    public CryptoFetcherService(StringRedisTemplate redisTemplate) {
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
                redisTemplate.opsForValue().set("crypto:latest", data, 1, TimeUnit.HOURS);
                logger.info("Stored crypto:latest in Redis ({} bytes)", data.length());

                // Extract metadata and active pairs
                JsonNode root = mapper.readTree(data);
                JsonNode dataArray = root.get("data");
                if (dataArray == null || !dataArray.isArray()) {
                    logger.warn("Invalid CMC data structure");
                    return;
                }

                List<String> ids = new ArrayList<>();
                Set<String> activePairs = new HashSet<>();
                for (JsonNode coin : dataArray) {
                    String id = coin.get("id").asText();
                    String symbol = coin.get("symbol").asText();
                    String pair = symbol + "-USDT";
                    JsonNode usdQuote = coin.get("quote").get("USD");
                    double marketCap = usdQuote.has("market_cap") ? usdQuote.get("market_cap").asDouble() : 0.0;
                    double circulatingSupply = coin.has("circulating_supply") ? coin.get("circulating_supply").asDouble() : 0.0;

                    // Store COININFO:{COIN}:* as strings
                    Map<String, String> coinInfo = new HashMap<>();
                    coinInfo.put("id", id);
                    coinInfo.put("name", coin.get("name").asText());
                    coinInfo.put("slug", coin.get("slug").asText());
                    coinInfo.put("circulating_supply", String.format("%.2f", circulatingSupply));
                    coinInfo.put("total_supply", coin.has("total_supply") ? String.format("%.2f", coin.get("total_supply").asDouble()) : "0");
                    coinInfo.put("max_supply", coin.has("max_supply") ? String.format("%.2f", coin.get("max_supply").asDouble()) : "0");
                    redisTemplate.opsForHash().putAll("COININFO:" + symbol, coinInfo);
                    redisTemplate.expire("COININFO:" + symbol, 1, TimeUnit.HOURS);

                    // Store market cap
                    redisTemplate.opsForValue().set("marketcap:" + symbol, String.format("%.2f", marketCap), 1, TimeUnit.HOURS);
                    activePairs.add(pair);
                    ids.add(id);
                }

                // Update active:pairs
                redisTemplate.delete("active:pairs");
                redisTemplate.opsForSet().add("active:pairs", activePairs.toArray(new String[0]));
                redisTemplate.expire("active:pairs", 1, TimeUnit.HOURS);
                logger.info("Updated active:pairs with {} pairs", activePairs.size());

                // Fetch logo and additional info
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
                                Map<String, String> extraInfo = new HashMap<>();
                                extraInfo.put("logo", coinInfo.get("logo").asText());
                                extraInfo.put("category", coinInfo.has("category") ? coinInfo.get("category").asText() : "");
                                extraInfo.put("tags", coinInfo.has("tags") ? mapper.writeValueAsString(coinInfo.get("tags")) : "");
                                extraInfo.put("urls:website", coinInfo.has("urls") && coinInfo.get("urls").has("website") ?
                                        mapper.writeValueAsString(coinInfo.get("urls").get("website")) : "");
                                extraInfo.put("urls:explorer", coinInfo.has("urls") && coinInfo.get("urls").has("explorer") ?
                                        mapper.writeValueAsString(coinInfo.get("urls").get("explorer")) : "");
                                extraInfo.put("urls:technical_doc", coinInfo.has("urls") && coinInfo.get("urls").has("technical_doc") ?
                                        mapper.writeValueAsString(coinInfo.get("urls").get("technical_doc")) : "");
                                extraInfo.put("urls:twitter", coinInfo.has("urls") && coinInfo.get("urls").has("twitter") ?
                                        mapper.writeValueAsString(coinInfo.get("urls").get("twitter")) : "");
                                redisTemplate.opsForHash().putAll("COININFO:" + symbol, extraInfo);
                                redisTemplate.expire("COININFO:" + symbol, 1, TimeUnit.HOURS);
                                logoCount++;
                            }
                        }
                        logger.info("Stored COININFO for {} of {} coins", logoCount, ids.size());
                    }
                }
            } else if (response.getStatusCode() == HttpStatus.TOO_MANY_REQUESTS) {
                logger.warn("CMC rate limit exceeded, retrying in 5 minutes");
                Mono.delay(Duration.ofMinutes(5)).subscribe(v -> fetchAndBroadcastCryptoData());
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

    // Initialize default COININFO for critical pairs
    @PostConstruct
    public void initializeDefaultCoinInfo() {
        String[] criticalSymbols = {"BTC", "ETH", "SOL", "XRP", "BNB", "ADA"};
        for (String symbol : criticalSymbols) {
            if (redisTemplate.opsForHash().size("COININFO:" + symbol) == 0) {
                Map<String, String> defaultInfo = new HashMap<>();
                defaultInfo.put("id", symbol);
                defaultInfo.put("name", symbol);
                defaultInfo.put("slug", symbol.toLowerCase());
                defaultInfo.put("circulating_supply", "0");
                defaultInfo.put("total_supply", "0");
                defaultInfo.put("max_supply", "0");
                defaultInfo.put("logo", "https://default.logo.png");
                redisTemplate.opsForHash().putAll("COININFO:" + symbol, defaultInfo);
                redisTemplate.expire("COININFO:" + symbol, 1, TimeUnit.HOURS);
                logger.info("Initialized default COININFO for {}", symbol);
            }
        }
    }
}