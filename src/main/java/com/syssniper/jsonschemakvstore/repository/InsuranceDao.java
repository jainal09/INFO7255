package com.syssniper.jsonschemakvstore.repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.syssniper.jsonschemakvstore.entity.InsurancePlan;
import com.syssniper.jsonschemakvstore.services.PlanValidationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.LinkedHashMap;

@Repository
public class InsuranceDao {
    // set the redis hash set key
    private static final String HASH_KEY = "InsurancePlan";

    @Autowired
    private final RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private final PlanValidationService jsonValidationService;

    public InsuranceDao(RedisTemplate<String, Object> redisTemplate, PlanValidationService jsonValidationService) {
        this.redisTemplate = redisTemplate;
        this.jsonValidationService = jsonValidationService;
    }


    public String save(InsurancePlan insurancePlan) {
        redisTemplate.opsForHash().put(HASH_KEY, insurancePlan.getId(), insurancePlan.getPlan_data());
        //validate the json data against the schema
        if (!jsonValidationService.validateJsonAgainstSchema(insurancePlan.getPlan_data().toString())) {
            return "Invalid JSON data";
        }
        return "Insurance plan saved.";
    }


    public LinkedHashMap find(String id) {
        Object result = redisTemplate.opsForHash().get(HASH_KEY, id);
        if (result instanceof LinkedHashMap) {
            return (LinkedHashMap) result;
        } else {
            // Handle the case where the result is not a JsonNode, for example:
            // return a default JsonNode or throw an exception
            return null; // Or return a default JsonNode, e.g., JsonNodeFactory.instance.nullNode()
        }
    }

    // find all insurance plans
    public Object[] findAll() {
        Collection<Object> values = redisTemplate.opsForHash().entries(HASH_KEY).values();
        return values.toArray();
    }


    public void update(InsurancePlan insurancePlan) {
        save(insurancePlan);
    }

    public boolean delete(String id) {
        // check if the key exists
        if (redisTemplate.opsForHash().hasKey(HASH_KEY, id)) {
            redisTemplate.opsForHash().delete(HASH_KEY, id);
            return true;
        }
        else {
            return false;
        }
    }
}
