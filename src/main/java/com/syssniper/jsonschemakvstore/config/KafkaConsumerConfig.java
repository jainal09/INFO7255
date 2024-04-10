package com.syssniper.jsonschemakvstore.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syssniper.jsonschemakvstore.entity.InsurancePlan;
import com.syssniper.jsonschemakvstore.services.ElasticsearchService;
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

    @Autowired
    private ElasticsearchService messageIndexService;

    @Bean
    public Consumer<Flux<JsonNode>> input() {
        String index = "insurance-plans";
        return messages -> messages.subscribe(
                message -> {
                    try {
                        String objectId = message.get("objectId").textValue();
                        messageIndexService.indexDocument(index, objectId, message);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                },
                // print trace if error occurs

                error -> {
                    System.err.println("Failed to index message: " + error.getMessage());
                    error.printStackTrace();
                }
        );
    }
}