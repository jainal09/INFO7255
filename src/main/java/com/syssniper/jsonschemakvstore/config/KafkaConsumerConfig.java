package com.syssniper.jsonschemakvstore.config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;

import java.util.function.Consumer;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public Consumer<Flux<String>> input() {
        return messages -> messages.subscribe(message -> {
            System.out.println("Received message: " + message);
            // Process the message here
        });
    }
}