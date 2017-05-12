package com.hortonworks.streamline.streams.cluster.service.metadata.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.Collections;
import java.util.List;

import javax.ws.rs.core.SecurityContext;

/**
 * Wrapper used to show proper JSON formatting
 */
@JsonPropertyOrder({"databases"})
public class HiveDatabases extends Metadata {
    private List<String> databases;

    public HiveDatabases(List<String> databases, SecurityContext securityContext) {
        super(securityContext);
        this.databases = databases;
    }

    public static HiveDatabases newInstance(List<String> databases, SecurityContext securityContext) {
        return databases == null ? new HiveDatabases(Collections.emptyList(), securityContext) : new HiveDatabases(databases, securityContext);
    }

    @JsonProperty("databases")
    public List<String> list() {
        return databases;
    }

    @Override
    public String toString() {
        return "{" +
                "databases=" + databases +
                '}';
    }
}
