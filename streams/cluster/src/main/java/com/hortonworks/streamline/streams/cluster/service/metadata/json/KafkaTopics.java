package com.hortonworks.streamline.streams.cluster.service.metadata.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.List;

import javax.ws.rs.core.SecurityContext;

/**
 * Wrapper used to show proper JSON formatting
 */
@JsonPropertyOrder({"topics"})
public class KafkaTopics extends Metadata {
    final List<String> topics;

    public KafkaTopics(List<String> topics, SecurityContext securityContext) {
        super(securityContext);
        this.topics = topics;
    }

    @JsonProperty("topics")
    public List<String> list() {
        return topics;
    }
}
