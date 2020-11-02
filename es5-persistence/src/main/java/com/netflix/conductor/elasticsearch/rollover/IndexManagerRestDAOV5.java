/*
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.elasticsearch.rollover;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.conductor.dao.es5.index.ElasticSearchRestDAOV5;
import com.netflix.conductor.elasticsearch.ElasticSearchConfiguration;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.unit.SizeUnit;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class IndexManagerRestDAOV5 implements IndexManager {

    private final ElasticSearchConfiguration config;
    private final ObjectMapper objectMapper;
    private final String indexNamePrefix;
    private final String indexAliasName;
    private final RestClient elasticSearchAdminClient;
    private final IndexNameProvider indexNameProvider;
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexManagerRestDAOV5.class);

    @Inject
    IndexManagerRestDAOV5(RestClient lowLevelRestClient,
                          ElasticSearchConfiguration config,
                          ObjectMapper objectMapper,
                          IndexNameProvider indexNameProvider) {
        this.config = config;
        this.objectMapper = objectMapper;
        this.indexNamePrefix = config.getIndexName();
        this.indexAliasName = config.getRolloverIndexAliasName();
        this.elasticSearchAdminClient = lowLevelRestClient;
        this.indexNameProvider = indexNameProvider;

        this.indexNameProvider.updateIndices(getAllIndexes());

        if (config.isRolloverIndexingEnabled()) {
            //FIXME: Change update frequency
            Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(this::rollOverIndex, 30, 60, TimeUnit.SECONDS);
            Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(this::updateIndex,30,60, TimeUnit.SECONDS);
        }
    }

    public void rollOverIndex() {
        try {
            String newIndexName = indexNameProvider.getIndexNameToCreate();
            String resourcePath = "/" + indexAliasName + "/_rollover/" + newIndexName;

            ObjectNode requestParams = objectMapper.createObjectNode();
            ObjectNode rolloverCondition = objectMapper.createObjectNode();

            if (config.getRolloverMaxAgeCondition() != null) {
                //FIXME: Change to HOURS after testing
                rolloverCondition.put("max_age", new TimeValue(config.getRolloverMaxAgeCondition(), TimeUnit.MINUTES).toString());
            }

            if (config.getRolloverMaxDocsCondition() != null) {
                rolloverCondition.put("max_docs", config.getRolloverMaxDocsCondition());
            }

            if (config.getRolloverMaxSizeCondition() != null) {
                //FIXME: Change to GBS after testing
                rolloverCondition.put("max_size", new SizeValue(config.getRolloverMaxSizeCondition(), SizeUnit.MEGA).toString());
            }

            requestParams.set("conditions", rolloverCondition);

            ObjectNode mappings = objectMapper.createObjectNode();


            InputStream stream = this.getClass().getResourceAsStream("/mappings_docType_workflow.json");
            ObjectNode node = objectMapper.readValue(stream, ObjectNode.class);
            mappings.put(ElasticSearchRestDAOV5.WORKFLOW_DOC_TYPE, node.get(ElasticSearchRestDAOV5.WORKFLOW_DOC_TYPE));

            ObjectNode taskMappingNode = objectMapper.readValue(this.getClass().getResourceAsStream("/mappings_docType_task.json"), ObjectNode.class);
            mappings.put(ElasticSearchRestDAOV5.TASK_DOC_TYPE, taskMappingNode.get(ElasticSearchRestDAOV5.TASK_DOC_TYPE));

            ObjectNode setting = objectMapper.createObjectNode();
            ObjectNode indexSetting = objectMapper.createObjectNode();

            indexSetting.put("number_of_shards", config.getElasticSearchIndexShardCount());
            indexSetting.put("number_of_replicas", config.getElasticSearchIndexReplicationCount());
            setting.set("index", indexSetting);

            ObjectNode requestBody = objectMapper.createObjectNode();

            requestBody.set("settings", setting);
            requestParams.set("mappings", mappings);

            Response rolloverResponse = elasticSearchAdminClient.performRequest(
                    ElasticSearchRestDAOV5.HttpMethod.POST,
                    resourcePath,
                    Collections.emptyMap(),
                    new NStringEntity(requestParams.toString(),ContentType.APPLICATION_JSON));

            if (rolloverResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                ObjectNode rolloverResult = objectMapper.readValue(EntityUtils.toString(rolloverResponse.getEntity()), ObjectNode.class);
                String IS_INDEX_ROLLED_OVER = "rolled_over";
                if (Boolean.parseBoolean(String.valueOf(rolloverResult.get(IS_INDEX_ROLLED_OVER)))) {
                    LOGGER.info("Successfully rolled over to new index : {}", newIndexName);

                    //Delete the older indices based on the config
                    if (config.isOldRolloverIndexDeletionEnabled()) {
                        deleteOldRolledOverIndex();
                    }
                }

                this.indexNameProvider.updateIndices(getAllIndexes());
            }
        } catch (Exception ex) {
            LOGGER.error("Exception in index rollover", ex);
        }
    }

    @Override
    public IndexNameProvider getIndexNameProvider() {
        return indexNameProvider;
    }

    @Override
    public boolean isAliasIndexExists() {
        boolean currentIndexName = false;

        try {
            Response indicesResponse = elasticSearchAdminClient
                    .performRequest("GET", "/_cat/aliases?h=i");

            if (indicesResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                {
                    currentIndexName = new BufferedReader(new InputStreamReader(indicesResponse.getEntity()
                            .getContent()))
                            .lines().findFirst().isPresent();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return currentIndexName;
    }

    public List<Index> getAllIndexes() {

        List<Index> indexNames = new ArrayList<>();

        try {
            Response indicesResponse = elasticSearchAdminClient
                    .performRequest("GET", "/_cat/indices?format=json&h=i,creation.date");

            if (indicesResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                {
                    indexNames = Arrays.stream(objectMapper.readValue(new InputStreamReader(indicesResponse.getEntity().getContent()),
                            Index[].class))
                            .filter(index -> index.getName().startsWith(indexNamePrefix))
                            .collect(Collectors.toList());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return indexNames;
    }

    private void deleteOldRolledOverIndex() {
        try {
            List<Index> indices = getAllIndexes();
            if (indices.size() > config.getMaxBackupRolloverIndexToKeep()) {
                String oldestIndexToDelete = Collections.min(indices).getName();
                String resourcePath = "/" + oldestIndexToDelete;

                Response response = elasticSearchAdminClient.performRequest(
                        ElasticSearchRestDAOV5.HttpMethod.DELETE,
                        resourcePath,
                        Collections.emptyMap());

                if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    LOGGER.info("Deleted the rolled over index : {}", oldestIndexToDelete);
                }
            }
        } catch (IOException ex) {
            LOGGER.error("Exception in deleting rolled over index", ex);
        }
    }

    @Override
    public void updateIndex() {
        this.indexNameProvider.updateIndices(getAllIndexes());
    }
}
