/**
  * Copyright 2017 Hortonworks.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at

  *   http://www.apache.org/licenses/LICENSE-2.0

  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
 **/
package com.hortonworks.streamline.streams.cluster.service.metadata;


import com.google.common.collect.ImmutableList;

import com.hortonworks.streamline.common.function.SupplierException;
import com.hortonworks.streamline.streams.catalog.exception.ServiceConfigurationNotFoundException;
import com.hortonworks.streamline.streams.catalog.exception.ServiceNotFoundException;
import com.hortonworks.streamline.streams.cluster.discovery.ambari.ServiceConfigurations;
import com.hortonworks.streamline.streams.cluster.service.EnvironmentService;
import com.hortonworks.streamline.streams.cluster.service.metadata.common.OverrideHadoopConfiguration;
import com.hortonworks.streamline.streams.cluster.service.metadata.common.Tables;
import com.hortonworks.streamline.streams.security.SecurityUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedActionException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.security.auth.Subject;
import javax.ws.rs.core.SecurityContext;

/**
 * Provides HBase databases tables metadata information using {@link org.apache.hadoop.hbase.client.HBaseAdmin}
 */
public class HBaseMetadataService implements AutoCloseable {
    private static final List<String> STREAMS_JSON_SCHEMA_CONFIG_HBASE_SITE =
            ImmutableList.copyOf(new String[] {ServiceConfigurations.HBASE.getConfNames()[2]});

    private Admin hBaseAdmin;
    private SecurityContext securityContext;
    private Subject subject;

    public HBaseMetadataService(Admin hBaseAdmin) {
        this(hBaseAdmin, null, null);
    }

    public HBaseMetadataService(Admin hBaseAdmin, SecurityContext securityContext, Subject subject) {
        this.hBaseAdmin = hBaseAdmin;
        this.securityContext = securityContext;
        this.subject = subject;
    }

    /**
     * Creates a new instance of {@link HBaseMetadataService} which delegates commands to {@link Admin}, which is instantiated with default {@link       //TODO DOCS
     * HBaseConfiguration} and {@code hbase-site.xml} config default properties overridden with the values set in hbase-site.xml
     * specified in cluster imported in the service pool (either manually or using Ambari)"
     */
    public static HBaseMetadataService newInstance(EnvironmentService environmentService, Long clusterId)
            throws IOException, ServiceConfigurationNotFoundException, ServiceNotFoundException {

        return HBaseMetadataService.newInstance(overrideConfig(HBaseConfiguration.create(), environmentService, clusterId));
    }

    public static HBaseMetadataService newInstance(Configuration hbaseConfig)
            throws IOException, ServiceConfigurationNotFoundException, ServiceNotFoundException {
        return new HBaseMetadataService(ConnectionFactory.createConnection(hbaseConfig).getAdmin());
    }

    /**
     * Creates a new instance of {@link HBaseMetadataService} which delegates to {@link Admin} instantiated  with the provided       //TODO DOCS
     * {@link HBaseConfiguration} and {@code hbase-site.xml} config related properties overridden with the values set in the
     * hbase-site config serialized in "streams json"
     */
    public static HBaseMetadataService newInstance(EnvironmentService environmentService, Long clusterId,
                                                   SecurityContext securityContext, Subject subject)
            throws IOException, ServiceConfigurationNotFoundException, ServiceNotFoundException {

        return HBaseMetadataService.newInstance(
                overrideConfig(HBaseConfiguration.create(), environmentService, clusterId),
                securityContext, subject);
    }

    public static HBaseMetadataService newInstance(Configuration hbaseConfig, SecurityContext securityContext, Subject subject)
                throws IOException, ServiceConfigurationNotFoundException, ServiceNotFoundException {

        UserGroupInformation.setConfiguration(hbaseConfig);

        return new HBaseMetadataService(ConnectionFactory.createConnection(hbaseConfig,
                    User.create(UserGroupInformation.getUGIFromSubject(subject)))
                .getAdmin(), securityContext, subject);
    }

    private static Configuration overrideConfig(Configuration hbaseConfig, EnvironmentService environmentService, Long clusterId)
            throws IOException, ServiceConfigurationNotFoundException, ServiceNotFoundException {
        return OverrideHadoopConfiguration.override(environmentService, clusterId,
                ServiceConfigurations.HBASE, STREAMS_JSON_SCHEMA_CONFIG_HBASE_SITE, hbaseConfig);
    }

    /**
     * @return All tables for all namespaces
     */
    public Tables getHBaseTables() throws Exception {
        TableName[] tableNames = executeSecure(() -> hBaseAdmin.listTableNames());
        return Tables.newInstance(tableNames);
    }

    /**
     * @param namespace Namespace for which to get table names
     * @return All tables for the namespace given as parameter
     */
    public Tables getHBaseTables(final String namespace) throws IOException, PrivilegedActionException {
        final TableName[] tableNames = executeSecure(() -> hBaseAdmin.listTableNamesByNamespace(namespace));
        return Tables.newInstance(tableNames);
    }

    /**
     * @return All namespaces
     */
    public Namespaces getHBaseNamespaces() throws IOException, PrivilegedActionException {
        return Namespaces.newInstance(executeSecure(() -> hBaseAdmin.listNamespaceDescriptors()));
    }

    @Override
    public void close() throws Exception {
        executeSecure(() -> {
            final Connection connection = hBaseAdmin.getConnection();
            hBaseAdmin.close();
            connection.close();
            return null;
        });
    }

    private <T, E extends Exception> T executeSecure(SupplierException<T, E> action) throws PrivilegedActionException, E {
        return SecurityUtil.execute(action, securityContext, subject, true); //TODO
    }

    /*
        Create and delete methods useful for system tests. Left as package protected for now.
        These methods can be made public and exposed in REST API.
    */
    void createNamespace(String namespace) throws IOException {
        hBaseAdmin.createNamespace(NamespaceDescriptor.create(namespace).build());
    }

    void createTable(String namespace, String tableName, String familyName) throws IOException {
        hBaseAdmin.createTable(new HTableDescriptor(TableName.valueOf(namespace, tableName))
                .addFamily(new HColumnDescriptor(familyName)));
    }

    void deleteNamespace(String namespace) throws IOException {
        hBaseAdmin.deleteNamespace(namespace);
    }

    void deleteTable(String namespace, String tableName) throws IOException {
        hBaseAdmin.deleteTable(TableName.valueOf(namespace, tableName));
    }

    void disableTable(String namespace, String tableName) throws IOException {
        hBaseAdmin.disableTable(TableName.valueOf(namespace, tableName));
    }

    /**
     * Wrapper used to show proper JSON formatting
     */
    public static class Namespaces {
        private List<String> namespaces;

        public Namespaces(List<String> namespaces) {
            this.namespaces = namespaces;
        }

        public static Namespaces newInstance(NamespaceDescriptor[] namespaceDescriptors) {
            List<String> namespaces = Collections.emptyList();
            if (namespaceDescriptors != null) {
                namespaces = new ArrayList<>(namespaceDescriptors.length);
                for (NamespaceDescriptor namespace : namespaceDescriptors) {
                    namespaces.add(namespace.getName());
                }
            }
            return new Namespaces(namespaces);
        }

        public List<String> getNamespaces() {
            return namespaces;
        }
    }
}
