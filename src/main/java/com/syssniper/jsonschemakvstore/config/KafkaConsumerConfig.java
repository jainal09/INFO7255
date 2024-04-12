package com.syssniper.jsonschemakvstore.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.syssniper.jsonschemakvstore.services.ElasticsearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.util.function.Consumer;

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

                        ObjectMapper mapper = new ObjectMapper();
                        ObjectNode baseNode = mapper.createObjectNode();
                        baseNode.put("_org", message.get("_org").textValue());
                        baseNode.put("objectId", objectId);
                        baseNode.put("objectType", message.get("objectType").textValue());
                        baseNode.put("planType", message.get("planType").textValue());
                        baseNode.put("creationDate", message.get("creationDate").textValue());
                        messageIndexService.indexDocument(index, objectId, baseNode,"");

                        JsonNode planCostSharesNode = message.get("planCostShares");
                        ObjectMapper pcs_mapper = new ObjectMapper();
                        ObjectNode PCSObject = pcs_mapper.createObjectNode();
                        PCSObject.put("name", "planCostShares");
                        PCSObject.put("parent", baseNode.get("objectId").textValue());
                        ((ObjectNode) planCostSharesNode).set("plan_join", PCSObject);
                        System.out.println(planCostSharesNode);
                        messageIndexService.indexDocument(index, planCostSharesNode.get("objectId").textValue(), planCostSharesNode,objectId);

                        JsonNode linkedPlanServicesNode = message.get("linkedPlanServices");
                        for (JsonNode linkedPlanService : linkedPlanServicesNode) {
                            String linkedPlanServiceObjectId = linkedPlanService.get("objectId").textValue();
                            if (linkedPlanService.has("planServiceCostShares")) {
                                JsonNode planServiceCostSharesNode = linkedPlanService.get("planServiceCostShares");
                                ObjectMapper pscs_mapper = new ObjectMapper();
                                ObjectNode PSCSObject = pscs_mapper.createObjectNode();
                                PSCSObject.put("name", "planServiceCostShares");
                                PSCSObject.put("parent", linkedPlanService.get("objectId").textValue());
                                ((ObjectNode) planServiceCostSharesNode).set("plan_join", PSCSObject);
                                messageIndexService.indexDocument(index, planServiceCostSharesNode.get("objectId").textValue(), planServiceCostSharesNode, linkedPlanServiceObjectId);
                            }

                            if (linkedPlanService.has("linkedService")) {
                                JsonNode linkedServiceNode = linkedPlanService.get("linkedService");
                                ObjectMapper ls_mapper = new ObjectMapper();
                                ObjectNode LSObject = ls_mapper.createObjectNode();
                                LSObject.put("name", "linkedService");
                                LSObject.put("parent", linkedPlanService.get("objectId").textValue());
                                ((ObjectNode) linkedServiceNode).set("plan_join", LSObject);
                                messageIndexService.indexDocument(index, linkedServiceNode.get("objectId").textValue(),linkedServiceNode,linkedPlanServiceObjectId);
                            }

                            ObjectNode linkedPlanServiceNode = mapper.createObjectNode();
                            linkedPlanServiceNode.put("objectId", linkedPlanService.get("objectId").textValue());
                            linkedPlanServiceNode.put("objectType", linkedPlanService.get("objectType").textValue());
                            linkedPlanServiceNode.put("_org", linkedPlanService.get("_org").textValue());
                            ObjectMapper lps_mapper = new ObjectMapper();
                            ObjectNode LPSObject = lps_mapper.createObjectNode();
                            LPSObject.put("name", "linkedPlanServices");
                            LPSObject.put("parent", baseNode.get("objectId").textValue());
                            linkedPlanServiceNode.set("plan_join", LPSObject);
                            messageIndexService.indexDocument(index, linkedPlanServiceObjectId, (JsonNode) linkedPlanServiceNode,objectId);
                        }

                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                },
                error -> {
                    System.err.println("Failed to index message: " + error.getMessage());
                    error.printStackTrace();
                }
        );
    }
}