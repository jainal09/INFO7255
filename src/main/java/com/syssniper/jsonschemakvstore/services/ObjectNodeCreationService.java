package com.syssniper.jsonschemakvstore.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Service;

@Service
public class ObjectNodeCreationService {
        public JsonNode createParentNode(JsonNode message) {
        String objectId = message.get("objectId").textValue();
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode baseNode = mapper.createObjectNode();
        baseNode.put("_org", message.get("_org").textValue());
        baseNode.put("objectId", objectId);
        baseNode.put("objectType", message.get("objectType").textValue());
        baseNode.put("planType", message.get("planType").textValue());
        baseNode.put("creationDate", message.get("creationDate").textValue());
        return baseNode;
    }
    public JsonNode createPlanServiceCostSharesNode(JsonNode linkedPlanService) {
            JsonNode planServiceCostSharesNode = linkedPlanService.get("planserviceCostShares");
            ObjectMapper pscs_mapper = new ObjectMapper();
            ObjectNode PSCSObject = pscs_mapper.createObjectNode();
            PSCSObject.put("name", "planserviceCostShares");
            PSCSObject.put("parent", linkedPlanService.get("objectId").textValue());
            ((ObjectNode) planServiceCostSharesNode).set("plan_join", PSCSObject);
            return planServiceCostSharesNode;
    }

    public JsonNode createLinkedServiceNode(JsonNode linkedPlanService){
        JsonNode linkedServiceNode = linkedPlanService.get("linkedService");
        ObjectMapper ls_mapper = new ObjectMapper();
        ObjectNode LSObject = ls_mapper.createObjectNode();
        LSObject.put("name", "linkedService");
        LSObject.put("parent", linkedPlanService.get("objectId").textValue());
        ((ObjectNode) linkedServiceNode).set("plan_join", LSObject);
        return linkedServiceNode;
    }

    public JsonNode createLinkedPlanServiceNode(JsonNode patchRequestMessage, String parentId) {
            System.out.println("Inside createLinkedPlanServiceNode" + patchRequestMessage + " " + parentId);
            ObjectNode linkedPlanServiceNode =  new ObjectMapper().createObjectNode();
            linkedPlanServiceNode.put("objectId",
                    patchRequestMessage.get("objectId").textValue());
            linkedPlanServiceNode.put("objectType",
                    patchRequestMessage.get("objectType").textValue());
            linkedPlanServiceNode.put("_org",
                    patchRequestMessage.get("_org").textValue());
            ObjectMapper lps_mapper = new ObjectMapper();
            ObjectNode LPSObject = lps_mapper.createObjectNode();
            LPSObject.put("name", "linkedPlanServices");
            LPSObject.put("parent", parentId);
            linkedPlanServiceNode.set("plan_join", LPSObject);
            return linkedPlanServiceNode;
    }
}
