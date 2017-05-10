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

import com.google.common.collect.Lists;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.hortonworks.streamline.common.function.SupplierException;
import com.hortonworks.streamline.streams.catalog.exception.ServiceConfigurationNotFoundException;
import com.hortonworks.streamline.streams.catalog.exception.ServiceNotFoundException;
import com.hortonworks.streamline.streams.cluster.discovery.ambari.ServiceConfigurations;
import com.hortonworks.streamline.streams.cluster.service.EnvironmentService;
import com.hortonworks.streamline.streams.cluster.service.metadata.common.OverrideHadoopConfiguration;
import com.hortonworks.streamline.streams.cluster.service.metadata.common.Tables;
import com.hortonworks.streamline.streams.security.SecurityUtil;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedActionException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.security.auth.Subject;
import javax.ws.rs.core.SecurityContext;

/**
 * Provides Hive databases, and database tables metadata information using {@link HiveMetaStoreClient}
 */
public class HiveMetadataService implements AutoCloseable {
    protected static final Logger LOG = LoggerFactory.getLogger(HiveMetadataService.class);

    private static final String STREAMS_JSON_SCHEMA_CONFIG_HIVE_METASTORE_SITE = ServiceConfigurations.HIVE.getConfNames()[3];
    private static final String STREAMS_JSON_SCHEMA_CONFIG_HIVE_SITE = ServiceConfigurations.HIVE.getConfNames()[6];

    private final HiveConf hiveConf;  // HiveConf used to create HiveMetaStoreClient. If this class is created with
                                      // the 3 parameter constructor, it is set to null
    private final HiveMetaStoreClient metaStoreClient;
    private final SecurityContext securityContext;
    private final Subject subject;

    public HiveMetadataService(HiveMetaStoreClient metaStoreClient, SecurityContext securityContext, Subject subject) {
        this(metaStoreClient, null, securityContext, subject);
    }

    /**
     * @param hiveConf The hive configuration used to instantiate {@link HiveMetaStoreClient}
     */
    private HiveMetadataService(HiveMetaStoreClient metaStoreClient, HiveConf hiveConf,
                                SecurityContext securityContext, Subject subject) {
        this.metaStoreClient = metaStoreClient;
        this.hiveConf = hiveConf;
        this.securityContext = securityContext;
        this.subject = subject;
        LOG.info("Created {}", this);
    }

    /**
     * Creates {@link HiveMetadataService}, which delegates to {@link HiveMetaStoreClient} instantiated with
     * default {@link HiveConf}, and {@code hivemetastore-site.xml} and {@code hive-site.xml} properties overridden
     * with the config for the cluster imported in the service pool (represented in "streams json")
     */
    public static HiveMetadataService newInstance(EnvironmentService environmentService, Long clusterId,
                                                  SecurityContext securityContext, Subject subject)
            throws MetaException, IOException, ServiceConfigurationNotFoundException,
                ServiceNotFoundException, PrivilegedActionException {

        return HiveMetadataService.newInstance(OverrideHadoopConfiguration.override(environmentService, clusterId,
                ServiceConfigurations.HIVE, getConfigNames(), new HiveConf()), securityContext, subject);
    }

    /**
     * Creates {@link HiveMetadataService}, which delegates to {@link HiveMetaStoreClient}
     * instantiated with the {@link HiveConf} provided using the first parameter
     */
    public static HiveMetadataService newInstance(HiveConf hiveConf, SecurityContext securityContext, Subject subject)
            throws MetaException, IOException, ServiceConfigurationNotFoundException,
                ServiceNotFoundException, PrivilegedActionException {


        UserGroupInformation.setConfiguration(hiveConf);
        UserGroupInformation.getUGIFromSubject(subject);    // Sets the User principal in this subject

        //TODO FIX the isSecure
        return new HiveMetadataService(SecurityUtil.execute(() -> new HiveMetaStoreClient(hiveConf),
                securityContext, subject, true), hiveConf, securityContext, subject);
    }

    private static List<String> getConfigNames() {
        return Lists.newArrayList(STREAMS_JSON_SCHEMA_CONFIG_HIVE_METASTORE_SITE,
                STREAMS_JSON_SCHEMA_CONFIG_HIVE_SITE);
    }

    /**
     * @return The table names for the database specified in the parameter
     */
    public Tables getHiveTables(String dbName) throws MetaException, PrivilegedActionException {
        final Tables tables = Tables.newInstance(executeSecure(() -> metaStoreClient.getAllTables(dbName)));
        LOG.debug("Hive database [{}] has {}", dbName, tables);
        return tables;
    }

    /**
     * @return The names of all databases in the MetaStore.
     */
    public Databases getHiveDatabases() throws MetaException, PrivilegedActionException {
        final Databases databases = Databases.newInstance(executeSecure(metaStoreClient::getAllDatabases), securityContext);
        LOG.debug("Hive has {}", databases);
        return databases;
    }

    @Override
    public void close() throws Exception {
        executeSecure(() -> {
            metaStoreClient.close();
            return null;
        });
    }

    private <T, E extends Exception> T executeSecure(SupplierException<T, E> action) throws PrivilegedActionException, E {
        return SecurityUtil.execute(action, securityContext, subject, true); //TODO
    }

    /*
     *   Create and delete methods useful for system tests. Left as package protected for now.
     *   These methods can be made public and exposed in REST API.
     */
    void createDatabase(String dbName, String description, String locationUri, Map<String, String> parameters) throws TException {
        metaStoreClient.createDatabase(new Database(dbName,description, locationUri, parameters));
    }

    void dropDatabase(String dbName) throws TException {
        metaStoreClient.dropDatabase(dbName);
    }

    void createTable(String tableName,
                     String dbName,
                     String owner,
                     int createTime,
                     int lastAccessTime,
                     int retention,
                     StorageDescriptor sd,
                     List<FieldSchema> partitionKeys,
                     Map<String,String> parameters,
                     String viewOriginalText,
                     String viewExpandedText,
                     String tableType) throws TException {

        metaStoreClient.createTable(new Table(
                tableName,
                dbName,
                owner,
                createTime,
                lastAccessTime,
                retention,
                sd,
                partitionKeys,
                parameters,
                viewOriginalText,
                viewExpandedText,
                tableType));
    }

    void dropTable(String dbName, String tableName) throws TException {
        metaStoreClient.dropTable(dbName, tableName);
    }

    /**
     * @return The instance of the {@link HiveMetaStoreClient} used to retrieve Hive databases and tables metadata
     */
    public HiveMetaStoreClient getMetaStoreClient() {
        return metaStoreClient;
    }

    /**
     * @return a copy of the {@link HiveConf} used to configure the {@link HiveMetaStoreClient} instance created
     * using the factory methods. null if this object was initialized using the
     * {@link HiveMetadataService#HiveMetadataService(HiveMetaStoreClient, SecurityContext, Subject)} constructor
     */
    public HiveConf getHiveConfCopy() {
        return hiveConf == null ? null : new HiveConf(hiveConf);
    }

    /**
     * Wrapper used to show proper JSON formatting
     */
    public static class Databases {
        private static final String AUTHRZ_MSG =
                "Authorization not enforced. Every authenticated user has access the entire metadata info";

        private List<String> databases;
        @JsonInclude(JsonInclude.Include.NON_NULL)
        private String msg;

        public Databases(List<String> databases, boolean isSecure) {
            this.databases = databases;
            if (isSecure) {
                msg = AUTHRZ_MSG;
            }
        }

        public static Databases newInstance(List<String> databases, SecurityContext securityContext) {
            final boolean isSecure = true;
//            final boolean isSecure = securityContext.isSecure();    //TODO
            return databases == null ? new Databases(Collections.emptyList(), isSecure) : new Databases(databases, isSecure);
        }

        @JsonProperty("databases")
        public List<String> asList() {
            return databases;
        }

        public String getMsg() {
            return msg;
        }

        @Override
        public String toString() {
            return "{" +
                    "databases=" + databases +
                    '}';
        }
    }

    @Override
    public String toString() {
        return "HiveMetadataService{" +
                "hiveConf=" + hiveConf +
                ", metaStoreClient=" + metaStoreClient +
                ", securityContext=" + securityContext +
                ", subject=" + subject +
                '}';
    }
}
