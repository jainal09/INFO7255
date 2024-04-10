package com.syssniper.jsonschemakvstore.config;

import com.fasterxml.jackson.core.JsonProcessingException;
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
    public Consumer<Flux<JsonNode>> input() {
        return messages -> messages.subscribe(this::saveToElasticsearch);
    }

    private void saveToElasticsearch(JsonNode message) {
        try {
            // Extract the objectId from the message
            String objectId = message.get("objectId").textValue();
            if (objectId == null) {
                throw new IllegalArgumentException("objectId is missing in the message");
            }

            // Create an InsurancePlan object from the JsonNode
            InsurancePlan insurancePlan = objectMapper.treeToValue(message, InsurancePlan.class);

            // Index the InsurancePlan object to Elasticsearch
            IndexQuery indexQuery = new IndexQueryBuilder()
                    .withId(objectId)
                    .withObject(insurancePlan)
                    .build();
            elasticsearchOperations.index(indexQuery, IndexCoordinates.of("insurance-plans"));
        } catch (Exception e) {
            // Handle exceptions, e.g., log the error and the message
            System.err.println("Failed to index message: " + message);
            e.printStackTrace();
        }
    }
}