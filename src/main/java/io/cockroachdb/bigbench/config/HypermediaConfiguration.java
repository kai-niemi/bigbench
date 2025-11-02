package io.cockroachdb.bigbench.config;

import java.time.Duration;

import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.pool.PoolConcurrencyPolicy;
import org.apache.hc.core5.pool.PoolReusePolicy;
import org.apache.hc.core5.util.Timeout;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.boot.web.client.RestTemplateCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import io.cockroachdb.bigbench.shell.support.HypermediaClient;

@Configuration
public class HypermediaConfiguration implements RestTemplateCustomizer {
    @Override
    public void customize(RestTemplate restTemplate) {
        final int maxConnPerRoute = Runtime.getRuntime().availableProcessors() * 8;
        final int maxTotal = maxConnPerRoute * 2;

        PoolingHttpClientConnectionManager connectionManager = PoolingHttpClientConnectionManagerBuilder.create()
                .setDefaultSocketConfig(SocketConfig.custom()
                        .setSoTimeout(Timeout.ofSeconds(10)).build())
                .setPoolConcurrencyPolicy(PoolConcurrencyPolicy.STRICT)
                .setConnPoolPolicy(PoolReusePolicy.LIFO)
                .setMaxConnTotal(maxTotal)
                .setMaxConnPerRoute(maxConnPerRoute)
                .build();

        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory(
                HttpClients.custom()
                        .setConnectionManager(connectionManager).build());
        factory.setConnectTimeout(Duration.ofSeconds(5));
        factory.setReadTimeout(Duration.ofSeconds(5));

        restTemplate.setRequestFactory(factory);
    }

    @Bean
    public HypermediaClient hypermediaClient(RestTemplateBuilder builder) {
        return new HypermediaClient(builder.build());
    }
}
