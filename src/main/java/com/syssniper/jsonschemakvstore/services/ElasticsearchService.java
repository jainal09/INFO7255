package com.syssniper.jsonschemakvstore.services;

import com.fasterxml.jackson.databind.JsonNode;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class ElasticsearchService {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchService.class);
    private final RestHighLevelClient esClient;

    @Autowired
    public ElasticsearchService(RestHighLevelClient esClient) {
        this.esClient = esClient;
    }

    public void indexDocument(String indexName, String documentId, JsonNode jsonNode, String routing) throws IOException {

        Request request = new Request("POST", "/" + indexName + "/_doc/" + documentId);
        request.setJsonEntity(jsonNode.toPrettyString());
        request.addParameter("routing", routing);
        esClient.getLowLevelClient().performRequest(request);
        logger.info("Document indexed successfully");
    }
}