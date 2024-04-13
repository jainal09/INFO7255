package com.syssniper.jsonschemakvstore.repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.syssniper.jsonschemakvstore.services.ObjectNodeCreationService;
import com.syssniper.jsonschemakvstore.services.PlanValidationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.LinkedHashMap;

@Repository
public class InsuranceDaoImpl implements InsuranceRepository {

    private final RedisTemplate<String, Object> redisTemplate;
    private final PlanValidationService jsonValidationService;
    private final StreamBridge streamBridge;

    @Autowired
    public InsuranceDaoImpl(RedisTemplate<String, Object> redisTemplate,
                            PlanValidationService jsonValidationService,
                            StreamBridge streamBridge) {
        this.redisTemplate = redisTemplate;
        this.jsonValidationService = jsonValidationService;
        this.streamBridge = streamBridge;
    }

    @Autowired
    public ObjectNodeCreationService objectNodeCreationService;

    @Override
    public String save(JsonNode insurancePlan) {
        String objectId = insurancePlan.get("objectId").textValue();
        return saveJsonData(insurancePlan, objectId);
    }

    private String saveJsonData(JsonNode insurancePlan, String objectId) {
        redisTemplate.opsForHash().put("planCostShares",
                objectId,
                insurancePlan.get("planCostShares"));
        redisTemplate.opsForHash().put("linkedPlanServices",
                objectId,
                insurancePlan.get("linkedPlanServices"));
        JsonNode insuranceNode = objectNodeCreationService.createParentNode(insurancePlan);
        redisTemplate.opsForHash().put("insurancePlan", insuranceNode.get("objectId").textValue(),
                insuranceNode);
        return "Insurance plan saved.";
    }

    public String validateJsonReq(JsonNode insurancePlan, String schemaPath) {
        if (!jsonValidationService.validateJsonAgainstSchema(insurancePlan.toString(), schemaPath)) {
            return "Invalid JSON data";
        }
        return null;
    }


    @Override
    public LinkedHashMap find(String id) {
        LinkedHashMap result = new LinkedHashMap();
        if (findById(id)) {
            Object result_planCostShares = redisTemplate.opsForHash().get("planCostShares", id);
            Object result_linkedPlanServices = redisTemplate.opsForHash().get("linkedPlanServices", id);
            LinkedHashMap result_insuranceData = (LinkedHashMap) redisTemplate.opsForHash().get("insurancePlan", id);
            generateInsuranceMap(result_insuranceData, result);
            result.put("planCostShares", result_planCostShares);
            result.put("linkedPlanServices", result_linkedPlanServices);
        }
        return result;
    }


public LinkedHashMap addNewLinkedPlanService(String id, JsonNode linkedPlanService) {
         String x = validateJsonReq(linkedPlanService, "/schemas/patch_schema.json");
        if (x != null) return null;
    ObjectMapper objectMapper = new ObjectMapper();
    LinkedHashMap result = find(id);
    if (result == null) {
        return null;
    }
    ArrayList<LinkedHashMap> linkedPlanServices = (ArrayList<LinkedHashMap>) result.get("linkedPlanServices");
    // Convert JsonNode to LinkedHashMap
    LinkedHashMap linkedPlanServiceMap = objectMapper.convertValue(linkedPlanService, LinkedHashMap.class);
    linkedPlanServices.add(linkedPlanServiceMap);
    result.put("linkedPlanServices", linkedPlanServices);
    // save the updated plan
    redisTemplate.opsForHash().put("linkedPlanServices", id, result.get("linkedPlanServices"));
    // convert the linkedPlanService to ObjectNode
    ObjectNode linkedPlanServiceNode = objectMapper.convertValue(linkedPlanServiceMap, ObjectNode.class);
    // add new value parentId to the linkedPlanService
    linkedPlanServiceNode.put("parentId", id);
    streamBridge.send("output-1", (JsonNode)linkedPlanServiceNode);
    return result;
}

    @Override
    public Object[] findAll() {
        LinkedHashMap result_linkedPlanServices = (LinkedHashMap)redisTemplate.opsForHash().entries("linkedPlanServices");
        LinkedHashMap result_planCostShares = (LinkedHashMap)redisTemplate.opsForHash().entries("planCostShares");
        LinkedHashMap result_insurancePlan = (LinkedHashMap)redisTemplate.opsForHash().entries("insurancePlan");

        ArrayList<LinkedHashMap> result = new ArrayList<>();
        for (Object key : result_linkedPlanServices.keySet()) {
            LinkedHashMap<String, Object> mergedMap = new LinkedHashMap<>();
            generateInsuranceMap((LinkedHashMap) result_insurancePlan.get(key), mergedMap);
            mergedMap.put("insurancePlan", result_insurancePlan.get(key));
            mergedMap.put("planCostShares", result_planCostShares.get(key));
            mergedMap.put("linkedPlanServices", result_linkedPlanServices.get(key));
            result.add(mergedMap);
        }

        return result.toArray();
    }

    private void generateInsuranceMap(LinkedHashMap result_insurancePlan, LinkedHashMap<String, Object> mergedMap) {
        mergedMap.put("_org", result_insurancePlan.get("_org"));
        mergedMap.put("objectId", result_insurancePlan.get("objectId"));
        mergedMap.put("objectType", result_insurancePlan.get("objectType"));
        mergedMap.put("planType",result_insurancePlan.get("planType"));
        mergedMap.put("creationDate", result_insurancePlan.get("creationDate"));
    }


    @Override
    public String update(String objectId, JsonNode insurancePlan) {
        String x = validateJsonReq(insurancePlan, "/schemas/post_schema.json");
        if (x != null) return "Invalid JSON data";
        if (findById(objectId)) {
            saveJsonData(insurancePlan, objectId);
            return "Insurance plan updated.";
        }
        return "Insurance plan not found.";

    }

    public boolean findById(String id) {
        return redisTemplate.opsForHash().hasKey("linkedPlanServices", id) &&
                redisTemplate.opsForHash().hasKey("planCostShares", id) &&
                redisTemplate.opsForHash().hasKey("insurancePlan", id);
    }

    @Override
    public boolean delete(String id) {
      if (findById(id)){
            redisTemplate.opsForHash().delete("linkedPlanServices", id);
            redisTemplate.opsForHash().delete("planCostShares", id);
            redisTemplate.opsForHash().delete("insurancePlan", id);
            return true;
        } else {
            return false;
        }
    }
}