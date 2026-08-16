package com.mohammadanas.saga.scheduler.config;

import java.time.Duration;
import org.springframework.boot.http.client.ClientHttpRequestFactoryBuilder;
import org.springframework.boot.http.client.ClientHttpRequestFactorySettings;
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
     *
     * <p>Built with {@link ClientHttpRequestFactoryBuilder}, which replaced the deprecated
     * {@code ClientHttpRequestFactories} in Spring Boot 3.4. {@code detect()} picks the best
     * available client from the classpath, exactly as the old helper did — this service
     * bundles no specific HTTP client, so that resolves to the JDK one. The settings type
     * moved packages too, from {@code org.springframework.boot.web.client} to
     * {@code org.springframework.boot.http.client}.
     */
    @Bean
    public RestClient orchestratorRestClient(SchedulerProperties properties) {
        Duration timeout = properties.getRequestTimeout();

        ClientHttpRequestFactorySettings settings = ClientHttpRequestFactorySettings.defaults()
                .withConnectTimeout(timeout)
                .withReadTimeout(timeout);

        return RestClient.builder()
                .baseUrl(properties.getOrchestratorBaseUrl())
                .requestFactory(ClientHttpRequestFactoryBuilder.detect().build(settings))
                .build();
    }
}
