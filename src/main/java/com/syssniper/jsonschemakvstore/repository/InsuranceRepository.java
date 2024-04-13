package com.syssniper.jsonschemakvstore.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.LinkedHashMap;

public interface InsuranceRepository {
    String save(JsonNode insurancePlan) throws JsonProcessingException;
    LinkedHashMap find(String id);
    Object[] findAll();
    String update(String objectId, JsonNode insurancePlan) throws JsonProcessingException;
    boolean delete(String id);
}


