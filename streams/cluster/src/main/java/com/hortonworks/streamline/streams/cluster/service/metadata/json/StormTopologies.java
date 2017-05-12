package com.hortonworks.streamline.streams.cluster.service.metadata.json;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.List;

import javax.ws.rs.core.SecurityContext;

/** Wrapper used to show proper JSON formatting
 * {@code
 *  {
 *   "topologies" : [ "A", "B", "C" ]
 *  }
 * }
 * */
@JsonPropertyOrder({"topologies"})
public class StormTopologies extends Metadata {
    private List<String> topologies;

    public StormTopologies(List<String> topologies) {
        this(topologies, null);
    }

    public StormTopologies(List<String> topologies, SecurityContext securityContext) {
        super(securityContext);
        this.topologies = topologies;
    }

    @JsonGetter("topologies")
    public List<String> list() {
        return topologies;
    }
}
