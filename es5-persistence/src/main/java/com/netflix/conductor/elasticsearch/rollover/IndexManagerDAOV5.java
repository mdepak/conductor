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
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class IndexManagerDAOV5 implements IndexManager {

    private final ElasticSearchConfiguration config;
    private final ObjectMapper objectMapper;
    private final String indexAliasName;
    private final Client elasticSearchClient;
    private final IndexNameProvider indexNameProvider;

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexManagerDAOV5.class);


    @Inject
    IndexManagerDAOV5(Client elasticSearchClient, ElasticSearchConfiguration config, ObjectMapper objectMapper, IndexNameProvider indexNameProvider) throws ExecutionException, InterruptedException {
        this.config = config;
        this.objectMapper = objectMapper;
        this.indexAliasName = config.getRolloverIndexAliasName();
        this.elasticSearchClient = elasticSearchClient;
        this.indexNameProvider = indexNameProvider;

        waitForHealthyES();
        this.indexNameProvider.updateIndices(getAllIndexes());

        if (config.isRolloverIndexingEnabled()) {
            //FIXME: Change update frequency
            Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(this::rollOverIndex, 60, 60, TimeUnit.SECONDS);

            if(config.isOldRolloverIndexDeletionEnabled()) {
                Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(this::deleteOldRolledOverIndex, 60, 60, TimeUnit.SECONDS);
            }
        }
    }

    private void waitForHealthyES() throws ExecutionException, InterruptedException {
        elasticSearchClient.admin()
                .cluster()
                .prepareHealth()
                .setTimeout(new TimeValue(1, TimeUnit.MINUTES))
                .setWaitForGreenStatus()
                .execute()
                .get();
    }

    public void rollOverIndex() {
        try {
            String newIndexName = indexNameProvider.getIndexNameToCreate();
            RolloverRequest rolloverRequest = new RolloverRequest(indexAliasName, newIndexName);

            CreateIndexRequest createIndexRequest = new CreateIndexRequest(newIndexName);
            createIndexRequest.alias(new Alias(indexAliasName));
            rolloverRequest.setCreateIndexRequest(createIndexRequest);

            createIndexRequest.settings(Settings.builder()
                    .put("index.number_of_shards", config.getElasticSearchIndexShardCount())
                    .put("index.number_of_replicas", config.getElasticSearchIndexReplicationCount())
            );

            if (config.getRolloverMaxAgeCondition() != null) {
                //FIXME: Change to HOURS after testing
                rolloverRequest.addMaxIndexAgeCondition(new TimeValue(config.getRolloverMaxAgeCondition(), TimeUnit.MINUTES));
            }

            if (config.getRolloverMaxDocsCondition() != null) {
                rolloverRequest.addMaxIndexDocsCondition(config.getRolloverMaxDocsCondition());
            }

            LOGGER.trace("Trying to rollover the index to ");
            RolloverResponse rolloverResponse = elasticSearchClient.admin()
                    .indices().rolloversIndex(rolloverRequest).actionGet();

            if (rolloverResponse.isRolledOver()) {
                LOGGER.info("Successfully created rollover index with new index: {} for old index : {}", newIndexName, rolloverResponse.getOldIndex());
                //Rollover is successful; Update the workflow & task mappings
                updateIndexMappings(newIndexName);
                indexNameProvider.updateIndices(getAllIndexes());
            }
        } catch (Exception ex) {
            LOGGER.error("Exception in index rollover", ex);
        }
    }

    private void updateIndexMappings(String newIndexName){
        putMappings(newIndexName, "/mappings_docType_workflow.json",ElasticSearchRestDAOV5.WORKFLOW_DOC_TYPE);
        putMappings(newIndexName, "/mappings_docType_task.json",ElasticSearchRestDAOV5.TASK_DOC_TYPE);
    }

    private void putMappings(String indexName, String documentMappingFile, String documentType) {
        try {
            PutMappingRequest putMappingRequest = new PutMappingRequest(indexName);
            ObjectNode node = objectMapper.readValue(this.getClass().getResourceAsStream(documentMappingFile), ObjectNode.class);
            putMappingRequest.source(node.get(documentType).toString());

            PutMappingResponse response = elasticSearchClient.admin().indices().putMapping(putMappingRequest).actionGet();
            LOGGER.info("PutMapping request for index {} isAcknowledged: {}", indexName, response.isAcknowledged());
        } catch (IOException ex) {
            LOGGER.error("Exception in adding mapping for the index " + indexName + " for document type " + documentType, ex);
        }
    }

    @Override
    public IndexNameProvider getIndexNameProvider() {
        return indexNameProvider;
    }

    @Override
    public boolean isAliasIndexExists() {
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest();
        getAliasesRequest.aliases(config.getRolloverIndexAliasName());

        AliasesExistResponse response = elasticSearchClient.admin().indices().aliasesExist(getAliasesRequest).actionGet();
        return response.isExists();
    }

    public List<Index> getAllIndexes() {
        GetIndexRequest request = new GetIndexRequest();
        request.addFeatures(GetIndexRequest.Feature.SETTINGS);

        GetIndexResponse response = elasticSearchClient.admin().indices().getIndex(request).actionGet();

        return Arrays.stream(response.getIndices())
                .filter(indexName -> indexName.startsWith(config.getRolloverIndexNamePrefix()))
                .map(indexName -> new Index(indexName, Long.parseLong(response.getSettings().get(indexName).get("index.creation_date"))))
                .collect(Collectors.toList());
    }

    public void deleteOldRolledOverIndex() {
        List<Index> indices = getAllIndexes();
        if (indices.size() > config.getMaxBackupRolloverIndexToKeep()) {
            String oldestIndexToDelete = Collections.min(indices).getName();

            LOGGER.debug("Trying to delete the old rolled over index : {}", oldestIndexToDelete);
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(oldestIndexToDelete);
            DeleteIndexResponse response = elasticSearchClient.admin().indices().delete(deleteIndexRequest).actionGet();
            if (response.isAcknowledged()) {
                LOGGER.info("Deleted the rolled over index : {}", oldestIndexToDelete);
            }
        }
    }

    @Override
    public void updateIndex() {
        this.indexNameProvider.updateIndices(getAllIndexes());
    }
}
