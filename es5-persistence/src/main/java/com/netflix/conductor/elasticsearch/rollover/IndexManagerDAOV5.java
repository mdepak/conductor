package com.netflix.conductor.elasticsearch.rollover;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.conductor.dao.es5.index.ElasticSearchRestDAOV5;
import com.netflix.conductor.elasticsearch.ElasticSearchConfiguration;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class IndexManagerDAOV5 implements IndexManager {

    private final ElasticSearchConfiguration config;
    private final ObjectMapper objectMapper;
    private final String indexNamePrefix;
    private final String indexAliasName;
    private final RestClient elasticSearchAdminClient;
    private final IndexNameProvider indexNameProvider;


    private static final TimeZone GMT = TimeZone.getTimeZone("GMT");
    private static final SimpleDateFormat CONDUCTOR_INDEX_DATE_FORMAT= new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexManagerDAOV5.class);

    static
    {
        CONDUCTOR_INDEX_DATE_FORMAT.setTimeZone(GMT);
    }

    @Inject
    IndexManagerDAOV5(RestClient lowLevelRestClient, ElasticSearchConfiguration config, ObjectMapper objectMapper, IndexNameProvider indexNameProvider)
    {
        this.config = config;
        this.objectMapper = objectMapper;
        this.indexNamePrefix = config.getIndexName();
        this.indexAliasName = config.getElasticSearchRolloverIndexAliasName();
        this.elasticSearchAdminClient = lowLevelRestClient;
        this.indexNameProvider = indexNameProvider;


        this.indexNameProvider.updateIndices(getAllIndexes());

        if(config.isRolloverIndexingEnabled())
        {
            //FIXME: Change update frequency
            Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(this::rollOverIndex, 30, 60, TimeUnit.SECONDS);
        }
    }


    public void rollOverIndex() {
        try {
            String newIndexName = this.indexNamePrefix + "_" + CONDUCTOR_INDEX_DATE_FORMAT.format(new Date()) + "-1";
            String resourcePath = "/" + indexAliasName + "/_rollover/" + newIndexName;

            ObjectNode requestParams = objectMapper.createObjectNode();
            ObjectNode rolloverCondition = objectMapper.createObjectNode();

            if (config.getElasticSearchRolloverMaxAgeCondition() != null) {
                rolloverCondition.put("max_age", config.getElasticSearchRolloverMaxAgeCondition());
            }

            if (config.getElasticSearchRolloverMaxDocsCondition() != null) {
                rolloverCondition.put("max_docs", Integer.parseInt(config.getElasticSearchRolloverMaxDocsCondition()));
            }

            if (config.getElasticSearchRolloverMaxSizeCondition() != null) {
                rolloverCondition.put("max_size", config.getElasticSearchRolloverMaxSizeCondition());
            }

            requestParams.set("conditions", rolloverCondition);

            ObjectNode mappings = objectMapper.createObjectNode();


            InputStream stream =this.getClass().getResourceAsStream("/mappings_docType_workflow.json");
            ObjectNode node = objectMapper.readValue(stream, ObjectNode.class);
            mappings.put(ElasticSearchRestDAOV5.WORKFLOW_DOC_TYPE, node.get(ElasticSearchRestDAOV5.WORKFLOW_DOC_TYPE));

            ObjectNode taskMappingNode = objectMapper.readValue(this.getClass().getResourceAsStream("/mappings_docType_task.json"), ObjectNode.class);
            mappings.put(ElasticSearchRestDAOV5.TASK_DOC_TYPE, taskMappingNode.get(ElasticSearchRestDAOV5.TASK_DOC_TYPE));

            requestParams.set("mappings", mappings);

            Response rolloverResponse = elasticSearchAdminClient.performRequest(ElasticSearchRestDAOV5.HttpMethod.POST, resourcePath, Collections.emptyMap(),
                    new NStringEntity(requestParams.toString(), ContentType.APPLICATION_JSON));

            if (rolloverResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String responseBody = EntityUtils.toString(rolloverResponse.getEntity());
                RolloverResponse rolloverResult = objectMapper.readValue(responseBody, RolloverResponse.class);

                this.indexNameProvider.updateIndices(getAllIndexes());
                LOGGER.info("Index rolled over successful" + rolloverResult.getNewIndex());
                // Add Mappings for the workflow document type
                try {
//                    addMappingToIndex(newIndexName, WORKFLOW_DOC_TYPE, "/mappings_docType_workflow.json");
                } catch (Exception e) {
                    LOGGER.error("Failed to add {} mapping", ElasticSearchRestDAOV5.WORKFLOW_DOC_TYPE);
                }

                //Add Mappings for task document type
            /*    try {
                    addMappingToIndex(newIndexName, TASK_DOC_TYPE, "/mappings_docType_task.json");
                } catch (IOException e) {
                    logger.error("Failed to add {} mapping", TASK_DOC_TYPE);
                }*/
            }
            LOGGER.info("Rolled over '{}' index", newIndexName);


        } catch (Exception ex) {
            LOGGER.error("Exception in index rollover", ex);
        }
    }

    @Override
    public IndexNameProvider getIndexNameProvider()
    {
        return indexNameProvider;
    }

    @Override
    public Optional<String> getCurrentAliasIndexName() {
        Optional<String> currentIndexName = Optional.empty();

        try {
            Response indicesResponse = elasticSearchAdminClient
                    .performRequest("GET", "/_cat/aliases?h=i");

            if (indicesResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                {
                    currentIndexName = new BufferedReader(new InputStreamReader(indicesResponse.getEntity()
                            .getContent()))
                            .lines().findFirst();
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

    //FIXME: Implement this method
    public void deleteIndex()
    {

    }
}
