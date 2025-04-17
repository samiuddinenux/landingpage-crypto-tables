package com.example.landingPage.config;

import com.example.landingPage.service.WebSocketSummaryService;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final WebSocketSummaryService summaryService;

    public WebSocketConfig(WebSocketSummaryService summaryService) {
        this.summaryService = summaryService;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(summaryService, "/websocket").setAllowedOrigins("*");
    }
}