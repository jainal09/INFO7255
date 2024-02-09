package com.syssniper.jsonschemakvstore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syssniper.jsonschemakvstore.entity.InsurancePlan;
import com.syssniper.jsonschemakvstore.repository.InsuranceDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import java.util.LinkedHashMap;

@SpringBootApplication
@RestController
@RequestMapping("/api")


public class JsonSchemaKvStoreApplication {

    @Autowired
    private InsuranceDao insuranceDao;

    public static void main(String[] args) {
        SpringApplication.run(JsonSchemaKvStoreApplication.class, args);
    }

    private String generateEtag(String planDataString) throws NoSuchAlgorithmException {
        System.out.println("planDataString: " + planDataString);
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] hash = md.digest(planDataString.getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        for (byte b : hash) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    @RequestMapping("/save")
    public ResponseEntity<String> save(@RequestBody InsurancePlan insurancePlan) throws NoSuchAlgorithmException {
        String result = insuranceDao.save(insurancePlan);
        // send 201 created status

        return ResponseEntity.status(HttpStatus.CREATED).header("ETag", generateEtag(insurancePlan.getPlan_data().toString())).body(result);
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> get(@PathVariable String id, @RequestHeader(value = HttpHeaders.IF_NONE_MATCH, required = false) String eTag) throws NoSuchAlgorithmException {
        System.out.println("id: " + id);
        LinkedHashMap plan = insuranceDao.find(id);
        if (plan == null) {
            return ResponseEntity.notFound().build();
        }

        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.valueToTree(plan);
        String currentEtag = generateEtag(jsonNode.toString()); // Assuming you have a method to generate the ETag
        if (eTag != null && eTag.equals(currentEtag)) {
            // ETag matches, return  304 Not Modified
            return ResponseEntity.status(HttpStatus.NOT_MODIFIED).build();
        } else {
            // ETag does not match or was not provided, return the resource with ETag
            return ResponseEntity.ok().header(HttpHeaders.ETAG, currentEtag).body(plan);
        }
    }

    @GetMapping("/all")
    public Object[] getAll() {
        return insuranceDao.findAll();
    }

    @RequestMapping("/update")
    public String update(@RequestBody InsurancePlan insurancePlan) {
        insuranceDao.update(insurancePlan);
        return "Insurance plan updated.";
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<String> delete(@PathVariable String id) {
        if(insuranceDao.delete(id)){
            return ResponseEntity.status(HttpStatus.ACCEPTED).body("Insurance plan deleted.");
        }
        else {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Insurance plan not found.");
        }
    }

}
