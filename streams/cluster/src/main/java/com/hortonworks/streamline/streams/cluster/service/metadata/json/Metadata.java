package com.hortonworks.streamline.streams.cluster.service.metadata.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.hortonworks.streamline.streams.security.SecurityUtil;

import javax.ws.rs.core.SecurityContext;

@JsonPropertyOrder({"authorized", "msg"})
public class Metadata {
    public static final String AUTHRZ_MSG =
            "Authorization not enforced. Every authenticated user has access to all metadata info";

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String msg;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Boolean authorized;

    public Metadata(String msg, Boolean authorized) {
        this.msg = msg;
        this.authorized = authorized;
    }

    public Metadata(SecurityContext securityContext) {
        if (SecurityUtil.isKerberosAuthenticated(securityContext)) {
            authorized = Boolean.TRUE;
            msg = "";
        } else {
            msg = Tables.AUTHRZ_MSG;
        }
    }

    public String getMsg() {
        return msg;
    }

    public Boolean getAuthorized() {
        return authorized;
    }
}
