package com.syssniper.jsonschemakvstore.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syssniper.jsonschemakvstore.repository.InsuranceDaoImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;

@RestController
@RequestMapping("/api")
public class InsuranceController {

    @Autowired
    private InsuranceDaoImpl insuranceImpl;

    private String generateEtag(String planDataString) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] hash = md.digest(planDataString.getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        for (byte b : hash) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    @RequestMapping("/save")
    public ResponseEntity<String> save(@RequestBody JsonNode insurancePlan) throws NoSuchAlgorithmException, JsonProcessingException {
        String result = insuranceImpl.save(insurancePlan);
        // send 201 created status or if incorrect json data send 400 bad request
        if (result.equals("Invalid JSON data")) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(result);
        }
        String etag = generateEtag(insurancePlan.toString());
        return ResponseEntity.status(HttpStatus.CREATED).header("ETag", etag).body(result);
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> get(@PathVariable String id, @RequestHeader(value = HttpHeaders.IF_NONE_MATCH, required = false) String eTag) throws NoSuchAlgorithmException {
        LinkedHashMap plan = insuranceImpl.find(id);
        if (plan.isEmpty()) {
            return ResponseEntity.notFound().build();
        }

        String currentEtag = getEtag(plan);
        if (eTag != null && eTag.equals(currentEtag)) {
            // ETag matches, return  304 Not Modified
            return ResponseEntity.status(HttpStatus.NOT_MODIFIED).build();
        } else {
            // ETag does not match or was not provided, return the resource with ETag
            return ResponseEntity.ok().header(HttpHeaders.ETAG, currentEtag).body(plan);
        }
    }

    @PatchMapping("/{id}")
    public ResponseEntity<?> patch(@PathVariable String id,
                                    @RequestBody JsonNode linkedPlanServices,
                                   @RequestHeader(value = HttpHeaders.IF_NONE_MATCH, required = false) String eTag)
            throws NoSuchAlgorithmException {
        LinkedHashMap plan = insuranceImpl.find(id);
        if (plan.isEmpty()) {
            return ResponseEntity.notFound().build();
        }

        String currentEtag = getEtag(plan);
        if (eTag != null && eTag.equals(currentEtag)) {
            // ETag matches, return  304 Not Modified
            LinkedHashMap body = insuranceImpl.addNewLinkedPlanService(id, linkedPlanServices);
            return ResponseEntity.status(HttpStatus.OK).header(HttpHeaders.ETAG, currentEtag).body(body);
        } else {
            // ETag does not match or was not provided, return missmatch error
            return ResponseEntity.status(HttpStatus.PRECONDITION_FAILED).build();
        }
    }

    private String getEtag(LinkedHashMap plan) throws NoSuchAlgorithmException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.valueToTree(plan);
        String currentEtag = generateEtag(jsonNode.toString()); // Assuming you have a method to generate the ETag
        return currentEtag;
    }

    @GetMapping("/all")
    public Object[] getAll() {
        return insuranceImpl.findAll();
    }

    @PutMapping("/{id}")
    public String update(@RequestBody JsonNode insurancePlan, @PathVariable String id) throws JsonProcessingException {
        insuranceImpl.update(id, insurancePlan);
        return "Insurance plan updated.";
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<String> delete(@PathVariable String id) {
        if (insuranceImpl.delete(id)) {
            return ResponseEntity.status(HttpStatus.ACCEPTED).body("Insurance plan deleted.");
        } else {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Insurance plan not found.");
        }
    }

}