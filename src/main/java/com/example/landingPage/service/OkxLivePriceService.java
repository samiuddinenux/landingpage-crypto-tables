package com.example.landingPage.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
public class OkxLivePriceService {

    private static final Logger logger = LoggerFactory.getLogger(OkxLivePriceService.class);

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ReactorNettyWebSocketClient client = new ReactorNettyWebSocketClient();
    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${okx.api.websocket-url}")
    private String websocketUrl;

    @Value("${okx.api.rest-url}")
    private String okxRestUrl;

    public OkxLivePriceService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @PostConstruct
    public void init() {
        String cmcData = redisTemplate.opsForValue().get("crypto:latest");
        if (cmcData == null) {
            logger.warn("crypto:latest not found, retrying in 10s");
            Mono.delay(Duration.ofSeconds(10)).subscribe(v -> init());
            return;
        }
        subscribeToTopPopular();
    }

    public void subscribeToTopPopular() {
        // Fetch valid OKX trading pairs
        Set<String> validPairs = fetchValidOkxPairs();
        if (validPairs.isEmpty()) {
            logger.error("No valid OKX pairs found, retrying in 30s");
            Mono.delay(Duration.ofSeconds(30)).subscribe(v -> subscribeToTopPopular());
            return;
        }

        List<String> defaultPairs = Arrays.asList(
                "BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "BNB-USDT", "ADA-USDT",
                "PEPE-USDT", "SHIB-USDT", "DOGE-USDT", "BONK-USDT"
        );

        List<String> topPopular = new ArrayList<>();
        String cmcData = redisTemplate.opsForValue().get("crypto:latest");
        if (cmcData != null) {
            try {
                JsonNode root = mapper.readTree(cmcData);
                JsonNode dataArray = root.get("data");
                if (dataArray != null && dataArray.isArray()) {
                    for (int i = 0; i < 20 && i < dataArray.size(); i++) {
                        String symbol = dataArray.get(i).get("symbol").asText();
                        String instId = symbol + "-USDT";
                        if (validPairs.contains(instId) && !topPopular.contains(instId)) {
                            topPopular.add(instId);
                        }
                    }
                }
            } catch (Exception e) {
                logger.warn("Failed to parse CMC data for subscriptions: {}", e.getMessage());
            }
        } else {
            logger.warn("crypto:latest missing, using default pairs");
        }

        // Add default pairs that are valid
        for (String pair : defaultPairs) {
            if (validPairs.contains(pair) && !topPopular.contains(pair)) {
                topPopular.add(pair);
            }
        }

        if (topPopular.isEmpty()) {
            logger.error("No valid pairs to subscribe to, retrying in 30s");
            Mono.delay(Duration.ofSeconds(30)).subscribe(v -> subscribeToTopPopular());
            return;
        }

        // Update active:pairs

        redisTemplate.opsForSet().add("active:pairs", topPopular.toArray(new String[0]));
        redisTemplate.expire("active:pairs", 1, TimeUnit.HOURS);

        List<Map<String, String>> argsList = new ArrayList<>();
        for (String instId : topPopular) {
            Map<String, String> arg = new HashMap<>();
            arg.put("channel", "tickers");
            arg.put("instId", instId);
            argsList.add(arg);
            redisTemplate.opsForSet().add("active:channels:" + instId, "tickers");
        }

        Map<String, Object> subscribeMsg = new HashMap<>();
        subscribeMsg.put("op", "subscribe");
        subscribeMsg.put("args", argsList);

        try {
            String payload = mapper.writeValueAsString(subscribeMsg);
            logger.info("Subscribing to tickers for {} pairs: {}", topPopular.size(), topPopular);

            client.execute(
                    URI.create(websocketUrl),
                    session -> {
                        logger.info("WebSocket connection established");
                        return session.send(Mono.just(session.textMessage(payload)))
                                .doOnSuccess(v -> logger.info("Subscribed to tickers"))
                                .thenMany(session.receive()
                                        .map(WebSocketMessage::getPayloadAsText)
                                        .doOnNext(message -> {
                                            logger.debug("Received WebSocket message: {}", message);
                                            handleWebSocketData(message);
                                        })
                                        .doOnError(e -> logger.error("WebSocket receive error: {}", e.getMessage())))
                                .then();
                    }
            ).doOnError(e -> {
                logger.error("WebSocket connection error: {}", e.getMessage());
                Mono.delay(Duration.ofSeconds(5 + new Random().nextInt(10))).subscribe(v -> subscribeToTopPopular());
            }).subscribe();
        } catch (Exception e) {
            logger.error("Subscription error: {}", e.getMessage());
            Mono.delay(Duration.ofSeconds(10)).subscribe(v -> subscribeToTopPopular());
        }
    }

    private Set<String> fetchValidOkxPairs() {
        try {
            ResponseEntity<String> response = restTemplate.exchange(
                    okxRestUrl + "?instType=SPOT",
                    HttpMethod.GET,
                    null,
                    String.class
            );
            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                JsonNode root = mapper.readTree(response.getBody());
                JsonNode dataArray = root.get("data");
                if (dataArray != null && dataArray.isArray()) {
                    Set<String> pairs = new HashSet<>();
                    for (JsonNode instrument : dataArray) {
                        String instId = instrument.get("instId").asText();
                        if (instId.endsWith("-USDT")) {
                            pairs.add(instId);
                        }
                    }
                    logger.info("Fetched {} valid OKX SPOT pairs", pairs.size());
                    return pairs;
                }
            }
            logger.warn("Failed to fetch OKX instruments: {}", response.getStatusCode());
        } catch (Exception e) {
            logger.error("Error fetching OKX instruments: {}", e.getMessage());
        }
        return Collections.emptySet();
    }

    private void handleWebSocketData(String message) {
        try {
            JsonNode root = mapper.readTree(message);
            if (root.has("event") && root.get("event").asText().equals("error")) {
                logger.error("WebSocket error: code={}, msg={}",
                        root.get("code").asText(), root.get("msg").asText());
                if (root.get("code").asText().equals("60018")) {
                    logger.info("Retrying subscription due to invalid channel or instId");
                    Mono.delay(Duration.ofSeconds(10)).subscribe(v -> subscribeToTopPopular());
                }
                return;
            }

            JsonNode dataArray = root.get("data");
            if (dataArray == null || !dataArray.isArray() || dataArray.isEmpty()) {
                logger.debug("No ticker data in message: {}", message);
                return;
            }

            String instId = root.get("arg").get("instId").asText();
            String symbol = instId.replace("-USDT", "");
            JsonNode tickerData = dataArray.get(0);

            // Store TICKER:{PAIR}
            String tickerJson = mapper.writeValueAsString(tickerData);
            redisTemplate.opsForValue().set("TICKER:" + instId, tickerJson, 1, TimeUnit.HOURS);

            // Extract common fields
            double price = tickerData.has("last") ? tickerData.get("last").asDouble() : 0.0;
            double open24h = tickerData.has("open24h") ? tickerData.get("open24h").asDouble() : 0.0;
            double change24h = open24h != 0 ? ((price - open24h) / open24h) * 100 : 0.0;
            double volume24h = tickerData.has("vol24h") ? tickerData.get("vol24h").asDouble() : 0.0;
            long timestamp = tickerData.has("ts") ? tickerData.get("ts").asLong() : System.currentTimeMillis();

            Object logoObj = redisTemplate.opsForHash().get("COININFO:" + symbol, "logo");
            Object circulatingSupplyObj = redisTemplate.opsForHash().get("COININFO:" + symbol, "circulating_supply");
            String logo = logoObj != null ? logoObj.toString() : "https://default.logo.png";
            String circulatingSupply = circulatingSupplyObj != null ? circulatingSupplyObj.toString() : "0";
            Double circulatingSupplyValue;
            try {
                circulatingSupplyValue = Double.parseDouble(circulatingSupply);
            } catch (NumberFormatException e) {
                logger.warn("Invalid circulating_supply for {}: {}", symbol, circulatingSupply);
                circulatingSupplyValue = 0.0;
            }

            // Construct popular data
            Map<String, Object> popularUpdate = new HashMap<>();
            popularUpdate.put("pair", instId);
            popularUpdate.put("price", price);
            popularUpdate.put("change24h", String.format("%.2f", change24h));
            popularUpdate.put("logo", logo);
            popularUpdate.put("circulatingSupply", circulatingSupplyValue);
            popularUpdate.put("timestamp", timestamp);
            popularUpdate.put("volume24h", volume24h);

            String popularJson = mapper.writeValueAsString(popularUpdate);
            redisTemplate.opsForValue().set("popular:" + instId, popularJson, 1, TimeUnit.HOURS);
            redisTemplate.opsForValue().set("okx:" + instId, String.format("%.6f", price), 1, TimeUnit.HOURS);
            logger.debug("Stored popular:{}: {}", instId, popularJson);

            // Construct pseudo-candlestick data for candle:latest:{pair}
            Map<String, Object> candleUpdate = new HashMap<>();
            candleUpdate.put("open", price); // Approximate open with current price
            candleUpdate.put("high", price); // Approximate high with current price
            candleUpdate.put("low", price);  // Approximate low with current price
            candleUpdate.put("close", price);
            candleUpdate.put("volume", 0.0); // Volume not available per second; use 0 or fetch from ticker
            candleUpdate.put("price", price);
            candleUpdate.put("timestamp", timestamp);
            candleUpdate.put("change24h", String.format("%.2f", change24h));
            candleUpdate.put("volume24h", volume24h);
            candleUpdate.put("logo", logo);
            candleUpdate.put("circulatingSupply", circulatingSupplyValue);

            String candleJson = mapper.writeValueAsString(candleUpdate);
            redisTemplate.opsForValue().set("candle:latest:" + instId, candleJson, 1, TimeUnit.HOURS);
            logger.debug("Stored candle:latest:{}: {}", instId, candleJson);

        } catch (Exception e) {
            logger.error("Error processing ticker: {}", e.getMessage());
        }
    }
}