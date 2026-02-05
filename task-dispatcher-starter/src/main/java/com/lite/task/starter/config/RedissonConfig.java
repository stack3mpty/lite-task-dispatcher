package com.lite.task.starter.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.TransportMode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Custom Redisson Configuration
 *
 * Programmatic configuration to resolve connection issues on macOS
 */
@Configuration
public class RedissonConfig {

    @Value("${spring.data.redis.host:127.0.0.1}")
    private String redisHost;

    @Value("${spring.data.redis.port:6379}")
    private int redisPort;

    @Value("${spring.data.redis.password:}")
    private String redisPassword;

    @Bean(destroyMethod = "shutdown")
    @Primary
    public RedissonClient redisson() {
        Config config = new Config();

        // Use NIO transport mode for better compatibility on macOS
        config.setTransportMode(TransportMode.NIO);

        String address = "redis://" + redisHost + ":" + redisPort;

        config.useSingleServer()
                .setAddress(address)
                .setConnectionMinimumIdleSize(1)
                .setConnectionPoolSize(5)
                .setConnectTimeout(30000)
                .setTimeout(10000)
                .setRetryAttempts(5)
                .setRetryInterval(3000);

        if (redisPassword != null && !redisPassword.isEmpty()) {
            config.useSingleServer().setPassword(redisPassword);
        }

        return Redisson.create(config);
    }
}
