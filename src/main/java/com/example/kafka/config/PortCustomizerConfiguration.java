package com.example.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.server.ConfigurableWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.stereotype.Component;
import org.springframework.util.SocketUtils;

@Component
@Slf4j
public class PortCustomizerConfiguration implements WebServerFactoryCustomizer<ConfigurableWebServerFactory> {
    @Value("${port.range:9000,9999}")
    private Integer[] portRange;

    @Override
    public void customize(ConfigurableWebServerFactory factory) {
        int port = SocketUtils.findAvailableTcpPort(portRange[0], portRange[1]);
        factory.setPort(port);
        System.setProperty("server.port", String.valueOf(port));
    }
}
