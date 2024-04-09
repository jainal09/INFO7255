package com.syssniper.jsonschemakvstore.entity;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.*;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.redis.core.RedisHash;


@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
@EqualsAndHashCode
@Document(indexName = "insurance")
@RedisHash("insurance-data")
public class InsurancePlan {
    private JsonNode plan_data;
}
