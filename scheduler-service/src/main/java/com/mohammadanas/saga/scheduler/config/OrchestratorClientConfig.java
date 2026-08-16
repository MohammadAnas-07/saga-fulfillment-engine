package com.mohammadanas.saga.scheduler.config;

import java.time.Duration;
import org.springframework.boot.web.client.ClientHttpRequestFactorySettings;
import org.springframework.boot.web.client.ClientHttpRequestFactories;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

@Configuration
public class OrchestratorClientConfig {

    /**
     * The client used to poll the orchestrator.
     *
     * <p>Both timeouts are set on purpose. A sweep that hangs on an unresponsive
     * orchestrator would hold every Redis lock it had taken until the TTL expired, turning
     * one slow dependency into a stalled sweep — so the request gives up and lets the next
     * pass retry, which is the behaviour §4 wants from a liveness mechanism.
     */
    @Bean
    public RestClient orchestratorRestClient(SchedulerProperties properties) {
        Duration timeout = properties.getRequestTimeout();

        return RestClient.builder()
                .baseUrl(properties.getOrchestratorBaseUrl())
                .requestFactory(ClientHttpRequestFactories.get(
                        ClientHttpRequestFactorySettings.DEFAULTS
                                .withConnectTimeout(timeout)
                                .withReadTimeout(timeout)))
                .build();
    }
}
