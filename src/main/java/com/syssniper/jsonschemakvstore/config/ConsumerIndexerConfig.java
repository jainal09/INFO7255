package com.syssniper.jsonschemakvstore.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.syssniper.jsonschemakvstore.services.ElasticsearchService;
import com.syssniper.jsonschemakvstore.services.ObjectNodeCreationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.util.function.Consumer;

@Configuration
public class ConsumerIndexerConfig {


    @Autowired
    private ElasticsearchService messageIndexService;

    @Autowired
    private ObjectNodeCreationService objectNodeCreationService;

    private final String index = "insurance";

    @Bean
    public Consumer<Flux<JsonNode>> input() {
        return messages -> messages.subscribe(
                message -> {
                    try {

                        JsonNode baseNode = objectNodeCreationService.createParentNode(message);
                        ObjectMapper base_mappper = new ObjectMapper();
                        ObjectNode BaseObject = base_mappper.createObjectNode();
                        BaseObject.put("name", "plan");
                        BaseObject.put("parent", " ");
                        ObjectNode baseNodeObj = (ObjectNode) baseNode;
                        baseNodeObj.set("plan_join", BaseObject);
                        String objectId = baseNode.get("objectId").textValue();
                        messageIndexService.indexDocument(index, objectId, baseNodeObj," ");

                        JsonNode planCostSharesNode = message.get("planCostShares");
                        ObjectMapper pcs_mapper = new ObjectMapper();
                        ObjectNode PCSObject = pcs_mapper.createObjectNode();
                        PCSObject.put("name", "planCostShares");
                        PCSObject.put("parent", baseNode.get("objectId").textValue());
                        ((ObjectNode) planCostSharesNode).set("plan_join", PCSObject);
                        System.out.println(planCostSharesNode);
                        messageIndexService.indexDocument(index, planCostSharesNode.get("objectId").textValue(),
                                planCostSharesNode, objectId);

                        JsonNode linkedPlanServicesNode = message.get("linkedPlanServices");
                        for (JsonNode linkedPlanService : linkedPlanServicesNode) {
                            String linkedPlanServiceObjectId = linkedPlanService.get("objectId").textValue();
                            if (linkedPlanService.has("planserviceCostShares")) {
                                JsonNode planServiceCostSharesNode = objectNodeCreationService.createPlanServiceCostSharesNode(linkedPlanService);
                                messageIndexService.indexDocument(index,
                                        planServiceCostSharesNode.get("objectId").textValue(),
                                        planServiceCostSharesNode, linkedPlanServiceObjectId);
                            }

                            if (linkedPlanService.has("linkedService")) {
                                JsonNode linkedServiceNode = objectNodeCreationService.createLinkedServiceNode(linkedPlanService);
                                messageIndexService.indexDocument(index,
                                        linkedServiceNode.get("objectId").textValue(),
                                        linkedServiceNode,linkedPlanServiceObjectId);
                            }

                            JsonNode linkedPlanServiceNode = objectNodeCreationService.createLinkedPlanServiceNode(linkedPlanService,
                                    baseNode.get("objectId").textValue());
                            messageIndexService.indexDocument(index, linkedPlanServiceObjectId,
                                    linkedPlanServiceNode,objectId);
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
    @Bean
      public Consumer<Flux<JsonNode>> anotherInput() {
        // print the incoming message
        return messages -> messages.subscribe(
                message -> {
                    System.out.println("Received message: " + message);
                    String linkedPlanServiceID = message.get("objectId").textValue();
                    System.out.println(linkedPlanServiceID);
                     if (message.has("planserviceCostShares")) {
                         JsonNode planServiceCostShareNode = objectNodeCreationService.createPlanServiceCostSharesNode(message);
                         try {
                             messageIndexService.indexDocument(index,
                                           planServiceCostShareNode.get("objectId").textValue(),
                                           planServiceCostShareNode, message.get("objectId").textValue());
                         } catch (IOException e) {
                             throw new RuntimeException(e);
                         }

                     }
                     if (message.has("linkedService")) {
                         JsonNode linkedServiceNode = objectNodeCreationService.createLinkedServiceNode(message);
                         try {
                             messageIndexService.indexDocument(index,
                                     linkedServiceNode.get("objectId").textValue(),
                                     linkedServiceNode, message.get("objectId").textValue());
                         } catch (IOException e) {
                             throw new RuntimeException(e);
                         }
                     }
                    System.out.println("indexing linkedPlanService");
                     JsonNode linkedPlanServiceNode = objectNodeCreationService.createLinkedPlanServiceNode(
                             message, message.get("parentId").textValue());
                        try {
                            messageIndexService.indexDocument(index, linkedPlanServiceID,
                                    linkedPlanServiceNode, message.get("parentId").textValue());
                        } catch (IOException e) {
                            throw new RuntimeException(e);

                        }
                },
                error -> {
                    System.err.println("Failed to process message: " + error.getMessage());
                    error.printStackTrace();
                }
        );
      }
}