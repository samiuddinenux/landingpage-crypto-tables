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

@Service
public class OkxLivePriceService {

    private final StringRedisTemplate redisTemplate;
    private final SimpMessagingTemplate messagingTemplate;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ReactorNettyWebSocketClient client = new ReactorNettyWebSocketClient();

    @Value("${okx.api.websocket-url}")
    private String websocketUrl;

    public OkxLivePriceService(StringRedisTemplate redisTemplate, SimpMessagingTemplate messagingTemplate) {
        this.redisTemplate = redisTemplate;
        this.messagingTemplate = messagingTemplate;
    }

    @PostConstruct
    public void init() {
        subscribeToTop6Popular();
    }

    public void subscribeToTop6Popular() {
        String cmcData = redisTemplate.opsForValue().get("crypto:latest");
        if (cmcData == null) return;

        try {
            JsonNode root = mapper.readTree(cmcData);
            JsonNode dataArray = root.get("data");
            if (dataArray == null || !dataArray.isArray()) return;

            List<String> top6Popular = new ArrayList<>();
            for (int i = 0; i < 6 && i < dataArray.size(); i++) {
                String symbol = dataArray.get(i).get("symbol").asText();
                top6Popular.add(symbol + "-USDT");
            }

            List<Map<String, String>> argsList = new ArrayList<>();
            for (String instId : top6Popular) {
                Map<String, String> arg = new HashMap<>();
                arg.put("channel", "tickers");
                arg.put("instId", instId);
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

            // Store update object with logo and circulating supply
            String symbol = instId.replace("-USDT", "");
            String logo = redisTemplate.opsForValue().get("logo:" + symbol);
            String circulatingSupply = redisTemplate.opsForValue().get("supply:" + symbol);

            Map<String, Object> update = new HashMap<>();
            update.put("pair", instId);
            update.put("price", okxPrice);
            update.put("logo", logo);
            update.put("circulatingSupply", circulatingSupply != null ? Double.parseDouble(circulatingSupply) : null);

            redisTemplate.opsForValue().set("popular:" + instId, mapper.writeValueAsString(update));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Scheduled(fixedRate = 1000)
    public void broadcastPopularUpdates() {
        for (String key : redisTemplate.keys("popular:*")) {
            String json = redisTemplate.opsForValue().get(key);
            if (json != null) {
                messagingTemplate.convertAndSend("/topic/popular", json);
            }
        }
    }
}