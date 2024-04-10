package com.syssniper.jsonschemakvstore.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syssniper.jsonschemakvstore.entity.InsurancePlan;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.document.Document;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;

import java.io.IOException;

@Configuration
public class KafkaConsumerConfig {

    private final ElasticsearchOperations elasticsearchOperations;
    private final ObjectMapper objectMapper;

    @Autowired
    public KafkaConsumerConfig(ElasticsearchOperations elasticsearchOperations, ObjectMapper objectMapper) {
        this.elasticsearchOperations = elasticsearchOperations;
        this.objectMapper = objectMapper;
    }

    @Bean
    public Consumer<Flux<String>> input() {
        return messages -> messages.subscribe(message -> {
            System.out.println("Received message: " + message);
            // Process the message here and save it in Elasticsearch
            saveToElasticsearch(message);
        });
    }

    private void saveToElasticsearch(String message) {
        try {
            // Parse the JSON message into JsonNode
            JsonNode jsonNode = objectMapper.readTree(message);
            // Save the JsonNode into Elasticsearch
            elasticsearchOperations.save(jsonNode);
        } catch (IOException e) {
            // Handle parsing error
            e.printStackTrace();
            System.err.println("Failed to parse message: " + message);
        }
    }
}