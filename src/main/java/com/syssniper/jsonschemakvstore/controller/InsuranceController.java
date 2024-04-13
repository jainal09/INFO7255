package com.syssniper.jsonschemakvstore.controller;

import com.fasterxml.jackson.databind.SerializationFeature;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syssniper.jsonschemakvstore.repository.InsuranceDaoImpl;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.LinkedHashMap;

@RestController
@RequestMapping("/api")
public class InsuranceController {

    private final StreamBridge streamBridge;

    @Autowired
    private InsuranceDaoImpl insuranceImpl;

    @Autowired
    public InsuranceController(StreamBridge streamBridge) {
        this.streamBridge = streamBridge;
    }
    private static final ObjectMapper SORTED_MAPPER = new ObjectMapper();
    static {
        SORTED_MAPPER.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    }

private String convertNode(final JsonNode node) throws JsonProcessingException {
    final Object obj = SORTED_MAPPER.treeToValue(node, Object.class);
    final String json = SORTED_MAPPER.writeValueAsString(obj);
    return json;
}

    @Autowired
    private RestHighLevelClient client;


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
        String x = insuranceImpl.validateJsonReq(insurancePlan, "/schemas/post_schema.json");
        if (x != null) return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Invalid Json");;
        LinkedHashMap plan = insuranceImpl.find(insurancePlan.get("objectId").textValue());
        if (!plan.isEmpty()) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body("Insurance plan already exists.");
        }
        String result = insuranceImpl.save(insurancePlan);
        streamBridge.send("output-0", insurancePlan);
        // send 201 created status or if incorrect json data send 400 bad request
        String etag = getEtag(insurancePlan);
        return ResponseEntity.status(HttpStatus.CREATED).header("ETag", etag).body(result);
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> get(@PathVariable String id, @RequestHeader(value = HttpHeaders.IF_NONE_MATCH, required = false) String eTag) throws NoSuchAlgorithmException, JsonProcessingException {
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
            throws NoSuchAlgorithmException, JsonProcessingException {
        LinkedHashMap plan = insuranceImpl.find(id);
        if (plan.isEmpty()) {
            return ResponseEntity.notFound().build();
        }

        String currentEtag = getEtag(plan);
        if (eTag != null && eTag.equals(currentEtag)) {
            // ETag matches, return  304 Not Modified
            LinkedHashMap body = insuranceImpl.addNewLinkedPlanService(id, linkedPlanServices);
            if(body == null){
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Invalid Json");
            }
             LinkedHashMap new_plan = insuranceImpl.find(id);
            String newEtag = getEtag(new_plan);
            return ResponseEntity.status(HttpStatus.OK).header(HttpHeaders.ETAG, newEtag).body(body);
        } else {
            // ETag does not match or was not provided, return missmatch error
            return ResponseEntity.status(HttpStatus.PRECONDITION_FAILED).build();
        }
    }

    private String getEtag(LinkedHashMap plan) throws NoSuchAlgorithmException, JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.valueToTree(plan);
        String planString = convertNode(jsonNode);
        System.out.println("Plan: " + planString);
        return generateEtag(planString);
    }
     private String getEtag(JsonNode plan) throws NoSuchAlgorithmException, JsonProcessingException {
        String planString = convertNode(plan);
         System.out.println("Plan: " + planString);
         return generateEtag(planString);
    }

    @GetMapping("/all")
    public ResponseEntity<Object[]> getAll(
            @RequestHeader(value = HttpHeaders.IF_NONE_MATCH, required = false) String eTag
    ) throws NoSuchAlgorithmException, IOException {
        ClusterHealthRequest request = new ClusterHealthRequest();
        ClusterHealthResponse response = client.cluster().health(request, RequestOptions.DEFAULT);
        System.out.println("Cluster Name: " + response.getClusterName());
        Object[] result = insuranceImpl.findAll();
        String currentEtag = generateEtag(Arrays.toString(result));
         if (eTag != null && !eTag.equals(currentEtag)) {
            // ETag matches, return  304 Not Modified
            return ResponseEntity.status(HttpStatus.NOT_MODIFIED).build();
        }
        return ResponseEntity.ok().header(HttpHeaders.ETAG, currentEtag).body(result);
    }

    @PutMapping("/{id}")
    public  ResponseEntity<String> update(@RequestBody JsonNode insurancePlan,
                                          @PathVariable String id,
                                           @RequestHeader(value = HttpHeaders.IF_NONE_MATCH) String eTag) throws NoSuchAlgorithmException, JsonProcessingException {
        LinkedHashMap plan = insuranceImpl.find(id);
        String currentEtag = getEtag(plan);
        if (eTag != null && !eTag.equals(currentEtag)) {
            // ETag matches, return  304 Not Modified
            return ResponseEntity.status(HttpStatus.PRECONDITION_FAILED).body("ETag mismatch");
        }

        String response = insuranceImpl.update(id, insurancePlan);
        if(response.equals("Invalid JSON data")){
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Json Schema Validation Failed");

        } else if (response.equals("Insurance plan not found.")) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Insurance plan not found.");
        } else {
                LinkedHashMap new_plan = insuranceImpl.find(id);
                String newEtag = getEtag(new_plan);
            return ResponseEntity.status(HttpStatus.ACCEPTED).header(HttpHeaders.ETAG, newEtag).body("Insurance plan updated.");

        }
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
