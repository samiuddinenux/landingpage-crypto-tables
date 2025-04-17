package com.example.landingPage.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class WebSocketController {

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    public WebSocketController(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @GetMapping("/gainers")
    public String getGainers() {
        String gainers = redisTemplate.opsForValue().get("gainers:json");
        return gainers != null ? gainers : "[]";
    }

    @GetMapping("/losers")
    public String getLosers() {
        String losers = redisTemplate.opsForValue().get("losers:json");
        return losers != null ? losers : "[]";
    }

    @GetMapping("/candlesticks/{pair}")
    public Map<String, Object> getCandlesticks(@PathVariable String pair) {
        Map<String, Object> response = new HashMap<>();
        response.put("pair", pair);
        Map<Object, Object> candles = redisTemplate.opsForHash().entries("CANDLES:" + pair + ":1m");
        Map<String, Object> parsedCandles = new HashMap<>();
        candles.forEach((key, value) -> {
            try {
                parsedCandles.put(key.toString(), mapper.readValue(value.toString(), Map.class));
            } catch (Exception e) {
                // Log parsing error
            }
        });
        response.put("candles", parsedCandles);
        return response;
    }
}