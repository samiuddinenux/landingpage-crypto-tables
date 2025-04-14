package com.example.landingPage.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class OkxTopMoversService {

    private final StringRedisTemplate redisTemplate;
    private final SimpMessagingTemplate messagingTemplate;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ReactorNettyWebSocketClient client = new ReactorNettyWebSocketClient();

    @Value("${okx.api.websocket-url}")
    private String websocketUrl;

    public OkxTopMoversService(StringRedisTemplate redisTemplate, SimpMessagingTemplate messagingTemplate) {
        this.redisTemplate = redisTemplate;
        this.messagingTemplate = messagingTemplate;
    }

    @PostConstruct
    public void init() {
        subscribeToAllPairs();
    }

    public void subscribeToAllPairs() {
        String cmcData = redisTemplate.opsForValue().get("crypto:latest");
        if (cmcData == null) return;

        try {
            JsonNode root = mapper.readTree(cmcData);
            JsonNode dataArray = root.get("data");
            if (dataArray == null || !dataArray.isArray()) return;

            List<String> allPairs = new ArrayList<>();
            for (JsonNode coin : dataArray) {
                String symbol = coin.get("symbol").asText();
                String instId = symbol + "-USDT";
                allPairs.add(instId);
            }

            int batchSize = 100;
            for (int i = 0; i < allPairs.size(); i += batchSize) {
                List<Map<String, String>> argsList = new ArrayList<>();
                for (int j = i; j < Math.min(i + batchSize, allPairs.size()); j++) {
                    Map<String, String> arg = new HashMap<>();
                    arg.put("channel", "tickers");
                    arg.put("instId", allPairs.get(j));
                    argsList.add(arg);
                }

                Map<String, Object> subscribeMsg = new HashMap<>();
                subscribeMsg.put("op", "subscribe");
                subscribeMsg.put("args", argsList);

                String payload;
                try {
                    payload = mapper.writeValueAsString(subscribeMsg);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }

                client.execute(
                        URI.create(websocketUrl),
                        session -> session.send(Mono.just(session.textMessage(payload)))
                                .thenMany(session.receive()
                                        .map(WebSocketMessage::getPayloadAsText)
                                        .doOnNext(this::handleWebSocketData))
                                .then()
                ).subscribe();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handleWebSocketData(String message) {
        try {
            JsonNode root = mapper.readTree(message);
            JsonNode dataArray = root.get("data");
            if (dataArray == null || !dataArray.isArray() || dataArray.isEmpty()) return;

            JsonNode data = dataArray.get(0);
            String instId = root.get("arg").get("instId").asText();
            double okxPrice = data.get("last").asDouble();

            redisTemplate.opsForValue().set("okx:" + instId, String.format("%.6f", okxPrice));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Scheduled(fixedRate = 5000)
    public void broadcastTopMovers() {
        try {
            List<Map<String, Object>> gainers = new ArrayList<>();
            List<Map<String, Object>> losers = new ArrayList<>();

            for (String key : redisTemplate.keys("change24h:*")) {
                String instId = key.replace("change24h:", "");
                String changeStr = redisTemplate.opsForValue().get(key);
                if (changeStr == null) continue;

                double change;
                try {
                    change = Double.parseDouble(changeStr);
                } catch (NumberFormatException e) {
                    continue;
                }

                String symbol = instId.replace("-USDT", "");
                String priceStr = redisTemplate.opsForValue().get("okx:" + instId);
                double price = priceStr != null ? Double.parseDouble(priceStr) : 0.0;
                String logo = redisTemplate.opsForValue().get("logo:" + symbol);
                String circulatingSupply = redisTemplate.opsForValue().get("supply:" + symbol);

                Map<String, Object> entry = new HashMap<>();
                entry.put("pair", instId);
                entry.put("change", change);
                entry.put("price", price);
                entry.put("logo", logo);
                entry.put("circulatingSupply", circulatingSupply != null ? Double.parseDouble(circulatingSupply) : null);

                if (change > 0) {
                    gainers.add(entry);
                } else if (change < 0) {
                    losers.add(entry);
                }
            }

            gainers.sort((a, b) -> Double.compare((Double) b.get("change"), (Double) a.get("change")));
            losers.sort((a, b) -> Double.compare((Double) a.get("change"), (Double) b.get("change")));

            gainers = gainers.stream().limit(6).collect(Collectors.toList());
            losers = losers.stream().limit(6).collect(Collectors.toList());

            messagingTemplate.convertAndSend("/topic/gainers", gainers);
            messagingTemplate.convertAndSend("/topic/losers", losers);

            redisTemplate.opsForValue().set("gainers:json", mapper.writeValueAsString(gainers));
            redisTemplate.opsForValue().set("losers:json", mapper.writeValueAsString(losers));

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}