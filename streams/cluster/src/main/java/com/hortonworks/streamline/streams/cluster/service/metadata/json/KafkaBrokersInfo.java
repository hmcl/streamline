package com.hortonworks.streamline.streams.cluster.service.metadata.json;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.hortonworks.streamline.streams.cluster.service.metadata.common.HostPort;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.ws.rs.core.SecurityContext;

/**
 * Wrapper used to show proper JSON formatting
 * <pre>
 * {@code
 * { "brokers" : [ { "host" : "H1", "port" : 23 },
 *                 { "host" : "H2", "port" : 23 },
 *                 { "host" : "H3", "port" : 23 }
 *               ]
 * }
 *
 * { "brokers" : [ { "id" : "1" }, { "id" : "2" }, { "id" : "3" } ] } }
 * </pre>
 */
@JsonPropertyOrder({"brokers"})
public class KafkaBrokersInfo<T> extends Metadata {
    private final List<T> brokers;

    public KafkaBrokersInfo(List<T> brokers) {
        this(brokers, null);
    }

    public KafkaBrokersInfo(List<T> brokers, SecurityContext securityContext) {
        super(securityContext);
        this.brokers = brokers;
    }

    public static KafkaBrokersInfo<HostPort> hostPort(List<String> hosts, Integer port, SecurityContext securityContext) {
        List<HostPort> hostsPorts = Collections.emptyList();
        if (hosts != null) {
            hostsPorts = new ArrayList<>(hosts.size());
            for (String host : hosts) {
                hostsPorts.add(new HostPort(host, port));
            }
        }
        return new KafkaBrokersInfo<>(hostsPorts, securityContext);
    }

    public static KafkaBrokersInfo<KafkaBrokersInfo.BrokerId> brokerIds(List<String> brokerIds, SecurityContext securityContext) {
        List<KafkaBrokersInfo.BrokerId> brokerIdsType = Collections.emptyList();
        if (brokerIds != null) {
            brokerIdsType = new ArrayList<>(brokerIds.size());
            for (String brokerId : brokerIds) {
                brokerIdsType.add(new KafkaBrokersInfo.BrokerId(brokerId));
            }
        }
        return new KafkaBrokersInfo<>(brokerIdsType, securityContext);
    }

    public static KafkaBrokersInfo<String> fromZk(List<String> brokerInfo, SecurityContext securityContext) {
        return brokerInfo == null
                ? new KafkaBrokersInfo<>(Collections.<String>emptyList(), securityContext)
                : new KafkaBrokersInfo<>(brokerInfo, securityContext);
    }

    public List<T> getBrokers() {
        return brokers;
    }

    public static class BrokerId {
        final String id;

        public BrokerId(String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }
    }
}
