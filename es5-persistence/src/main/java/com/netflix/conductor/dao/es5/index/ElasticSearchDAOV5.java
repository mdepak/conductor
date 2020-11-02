/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.netflix.conductor.dao.es5.index;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.common.run.TaskSummary;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.run.WorkflowSummary;
import com.netflix.conductor.common.utils.RetryUtil;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.core.execution.ApplicationException.Code;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.es5.index.query.parser.Expression;
import com.netflix.conductor.elasticsearch.ElasticSearchConfiguration;
import com.netflix.conductor.elasticsearch.query.parser.ParserException;
import com.netflix.conductor.elasticsearch.rollover.IndexManager;
import com.netflix.conductor.elasticsearch.rollover.RequestWrapper;
import com.netflix.conductor.elasticsearch.rollover.retry.listener.RetryListenerProvider;
import com.netflix.conductor.metrics.Monitors;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

@Trace
@Singleton
public class ElasticSearchDAOV5 implements IndexDAO {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchDAOV5.class);

    private static final String WORKFLOW_DOC_TYPE = "workflow";
    private static final String TASK_DOC_TYPE = "task";
    private static final String LOG_DOC_TYPE = "task_log";
    private static final String EVENT_DOC_TYPE = "event";
    private static final String MSG_DOC_TYPE = "message";

    private static final String className = ElasticSearchDAOV5.class.getSimpleName();

    private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyyMMWW");
    private static final TimeZone GMT = TimeZone.getTimeZone("GMT");
    private final int RETRY_COUNT;

    private final String indexName;
    private String logIndexName;
    private final String logIndexPrefix;
    private final ObjectMapper objectMapper;
    private final Client elasticSearchClient;
    private final ExecutorService executorService;
    private final ExecutorService logExecutorService;
    private final int archiveSearchBatchSize;
    private final ConcurrentHashMap<String, BulkRequests<BulkRequestBuilderWrapper>> bulkRequests;
    private final ConcurrentHashMap<String, BulkRequests<BulkUpdateRequestsWrapper>> bulkUpdateRequests;
    private final int indexBatchSize;
    private final int asyncBufferFlushTimeout;
    private final ElasticSearchConfiguration config;
    private final IndexManager indexManager;
    private final RetryListenerProvider retryListenerProvider;

    static {
        SIMPLE_DATE_FORMAT.setTimeZone(GMT);
    }

    @Inject
    public ElasticSearchDAOV5(Client elasticSearchClient, ElasticSearchConfiguration config,
                              ObjectMapper objectMapper, IndexManager indexManager, RetryListenerProvider retryListenerProvider) {
        this.objectMapper = objectMapper;
        this.elasticSearchClient = elasticSearchClient;
        this.indexName = config.getIndexName();
        this.logIndexPrefix = config.getTasklogIndexName();
        this.archiveSearchBatchSize = config.getArchiveSearchBatchSize();
        this.bulkRequests = new ConcurrentHashMap<>();
        this.bulkUpdateRequests = new ConcurrentHashMap<>();
        this.indexBatchSize = config.getIndexBatchSize();
        this.asyncBufferFlushTimeout = config.getAsyncBufferFlushTimeout();
        this.config = config;
        this.indexManager = indexManager;
        this.retryListenerProvider = retryListenerProvider;
        this.RETRY_COUNT = config.getIndexRetryCount();

        int corePoolSize = 4;
        int maximumPoolSize = config.getAsyncMaxPoolSize();
        long keepAliveTime = 1L;
        int workerQueueSize = config.getAsyncWorkerQueueSize();
        this.executorService = new ThreadPoolExecutor(corePoolSize,
            maximumPoolSize,
            keepAliveTime,
            TimeUnit.MINUTES,
            new LinkedBlockingQueue<>(workerQueueSize),
                (runnable, executor) -> {
                    logger.warn("Request {} to async dao discarded in executor {}", runnable, executor);
                    Monitors.recordDiscardedIndexingCount("indexQueue");
                });

        corePoolSize = 1;
        maximumPoolSize = 2;
        keepAliveTime = 30L;
        this.logExecutorService = new ThreadPoolExecutor(corePoolSize,
            maximumPoolSize,
            keepAliveTime,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(workerQueueSize),
            (runnable, executor) -> {
                logger.warn("Request {} to async log dao discarded in executor {}", runnable, executor);
                Monitors.recordDiscardedIndexingCount("logQueue");
            });

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(this::flushBulkRequests, 60, 30, TimeUnit.SECONDS);
    }

    @PreDestroy
    private void shutdown() {
        logger.info("Gracefully shutdown executor service");
        shutdownExecutorService(logExecutorService);
        shutdownExecutorService(executorService);
    }

    private void shutdownExecutorService(ExecutorService execService) {
        try {
            execService.shutdown();
            if (execService.awaitTermination(30, TimeUnit.SECONDS)) {
                logger.debug("tasks completed, shutting down");
            } else {
                logger.warn("Forcing shutdown after waiting for 30 seconds");
                execService.shutdownNow();
            }
        } catch (InterruptedException ie) {
            logger.warn("Shutdown interrupted, invoking shutdownNow on scheduledThreadPoolExecutor for delay queue");
            execService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void setup() throws Exception {
        elasticSearchClient.admin()
            .cluster()
            .prepareHealth()
            .setWaitForGreenStatus()
            .execute()
            .get();

        try {
            initIndex();
            updateLogIndexName();
            Executors.newScheduledThreadPool(1)
                .scheduleAtFixedRate(this::updateLogIndexName, 0, 1, TimeUnit.HOURS);
        } catch (Exception e) {
            logger.error("Error creating index templates", e);
        }

        if (!config.isRolloverIndexingEnabled() || !indexManager.isAliasIndexExists()) {
            String newIndexName = indexManager.getIndexNameProvider().getIndexNameToCreate();

            //1. Create the required index
            try {
                addIndex(newIndexName, config.isRolloverIndexingEnabled() ? config.getRolloverIndexAliasName() : null);
            } catch (Exception e) {
                logger.error("Failed to initialize index '{}'", indexName, e);
            }

            indexManager.updateIndex();

            //2. Add Mappings for the workflow document type
            try {
                addMappingToIndex(newIndexName, WORKFLOW_DOC_TYPE, "/mappings_docType_workflow.json");
            } catch (Exception e) {
                logger.error("Failed to add {} mapping", WORKFLOW_DOC_TYPE);
            }

            //3. Add Mappings for task document type
            try {
                addMappingToIndex(newIndexName, TASK_DOC_TYPE, "/mappings_docType_task.json");
            } catch (IOException e) {
                logger.error("Failed to add {} mapping", TASK_DOC_TYPE);
            }
        }
    }

    private void addIndex(String indexName, @Nullable String aliasName) {
        try {
            elasticSearchClient.admin()
                    .indices()
                    .prepareGetIndex()
                    .addIndices(indexName)
                    .execute()
                    .actionGet();
        } catch (IndexNotFoundException infe) {
            try {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);

                if(aliasName != null)
                {
                    createIndexRequest.alias(new Alias(aliasName));
                }
                createIndexRequest.settings(Settings.builder()
                        .put("index.number_of_shards", config.getElasticSearchIndexShardCount())
                        .put("index.number_of_replicas", config.getElasticSearchIndexReplicationCount())
                );

                elasticSearchClient.admin()
                        .indices()
                        .create(createIndexRequest)
                        .actionGet();
            } catch (ResourceAlreadyExistsException done) {
                // no-op
            }
        }
    }

    private void addMappingToIndex(String indexName, String mappingType, String mappingFilename)
        throws IOException {
        GetMappingsResponse getMappingsResponse = elasticSearchClient.admin()
            .indices()
            .prepareGetMappings(indexName)
            .addTypes(mappingType)
            .execute()
            .actionGet();

        if (getMappingsResponse.mappings().isEmpty()) {
            logger.info("Adding the mappings for type: {}", mappingType);
            InputStream stream = ElasticSearchDAOV5.class.getResourceAsStream(mappingFilename);
            byte[] bytes = IOUtils.toByteArray(stream);
            String source = new String(bytes);
            try {
                elasticSearchClient.admin()
                    .indices()
                    .preparePutMapping(indexName)
                    .setType(mappingType)
                    .setSource(source, XContentFactory.xContentType(source))
                    .execute()
                    .actionGet();
            } catch (Exception e) {
                logger.error("Failed to init index mappings for type: {}", mappingType, e);
            }
        }
    }

    private void updateLogIndexName() {
        this.logIndexName = this.logIndexPrefix + "_" + SIMPLE_DATE_FORMAT.format(new Date());

        try {
            elasticSearchClient.admin()
                .indices()
                .prepareGetIndex()
                .addIndices(logIndexName)
                .execute()
                .actionGet();
        } catch (IndexNotFoundException infe) {
            try {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(logIndexName);

                createIndexRequest.settings(Settings.builder()
                        .put("index.number_of_shards", config.getElasticSearchIndexShardCount())
                        .put("index.number_of_replicas", config.getElasticSearchIndexReplicationCount())
                );

                elasticSearchClient.admin()
                        .indices()
                        .create(createIndexRequest)
                        .actionGet();
            } catch (ResourceAlreadyExistsException ilee) {
                // no-op
            } catch (Exception e) {
                logger.error("Failed to update log index name: {}", logIndexName, e);
            }
        }
    }

    /**
     * Initializes the index with required templates and mappings.
     */
    private void initIndex() throws Exception {

        // 0. Add the tasklog template
        GetIndexTemplatesResponse result = elasticSearchClient.admin()
            .indices()
            .prepareGetTemplates("tasklog_template")
            .execute()
            .actionGet();

        if (result.getIndexTemplates().isEmpty()) {
            logger.info("Creating the index template 'tasklog_template'");
            InputStream stream = ElasticSearchDAOV5.class
                .getResourceAsStream("/template_tasklog.json");
            byte[] templateSource = IOUtils.toByteArray(stream);

            try {
                elasticSearchClient.admin()
                    .indices()
                    .preparePutTemplate("tasklog_template")
                    .setSource(templateSource, XContentType.JSON)
                    .execute()
                    .actionGet();
            } catch (Exception e) {
                logger.error("Failed to init tasklog_template", e);
            }
        }
    }

    @Override
    public void indexWorkflow(Workflow workflow) {
        try {
            long startTime = Instant.now().toEpochMilli();
            String id = workflow.getWorkflowId();
            WorkflowSummary summary = new WorkflowSummary(workflow);
            byte[] doc = objectMapper.writeValueAsBytes(summary);

            UpdateRequest request = new UpdateRequest(indexName, WORKFLOW_DOC_TYPE, id);
            request.doc(doc, XContentType.JSON);
            request.upsert(doc, XContentType.JSON);
            request.retryOnConflict(5);

            new RetryUtil<UpdateResponse>().retryOnException(
                () -> elasticSearchClient.update(request).actionGet(),
                null,
                null,
                RETRY_COUNT,
                "Indexing workflow document: " + workflow.getWorkflowId(),
                "indexWorkflow"
            );

            long endTime = Instant.now().toEpochMilli();
            logger.debug("Time taken {} for indexing workflow: {}", endTime - startTime, workflow.getWorkflowId());
            Monitors.recordESIndexTime("index_workflow", WORKFLOW_DOC_TYPE, endTime - startTime);
            Monitors.recordWorkerQueueSize("indexQueue", ((ThreadPoolExecutor) executorService).getQueue().size());
        } catch (Exception e) {
            Monitors.error(className, "indexWorkflow");
            logger.error("Failed to index workflow: {}", workflow.getWorkflowId(), e);
        }
    }

    @Override
    public CompletableFuture<Void> asyncIndexWorkflow(Workflow workflow) {
        return CompletableFuture.runAsync(() -> indexWorkflow(workflow), executorService);
    }

    @Override
    public void createTask(Task task) {
        try {
            long startTime = Instant.now().toEpochMilli();
            String id = task.getTaskId();
            TaskSummary summary = new TaskSummary(task);
            byte[] doc = objectMapper.writeValueAsBytes(summary);

            IndexRequest req = new IndexRequest(indexName, TASK_DOC_TYPE, id);
            req.source(doc, XContentType.JSON);
            logger.debug("Indexing task document: {} for workflow: {}" + id, task.getWorkflowInstanceId());
            indexObject(req, TASK_DOC_TYPE);
            long endTime = Instant.now().toEpochMilli();
            logger.debug("Time taken {} for  indexing task:{} in workflow: {}", endTime - startTime, task.getTaskId(), task.getWorkflowInstanceId());
            Monitors.recordESIndexTime("index_task", TASK_DOC_TYPE, endTime - startTime);
            Monitors.recordWorkerQueueSize("indexQueue", ((ThreadPoolExecutor) executorService).getQueue().size());
        } catch (Exception e) {
            logger.error("Failed to index task: {}", task.getTaskId(), e);
        }
    }

    @Override
    public void updateTask(Task task) {
        try {
            long startTime = Instant.now().toEpochMilli();
            String id = task.getTaskId();
            TaskSummary summary = new TaskSummary(task);
            byte[] doc = objectMapper.writeValueAsBytes(summary);

            UpdateRequest req = new UpdateRequest(indexName, TASK_DOC_TYPE, id);
            req.doc(doc, XContentType.JSON);
            logger.debug("Updating task document: {} for workflow: {}" + id, task.getWorkflowInstanceId());
            updateObject(req, TASK_DOC_TYPE, task.getScheduledTime());
            long endTime = Instant.now().toEpochMilli();
            logger.debug("Time taken {} for  update task:{} in workflow: {}", endTime - startTime, task.getTaskId(), task.getWorkflowInstanceId());
            Monitors.recordESIndexTime("update_task", TASK_DOC_TYPE, endTime - startTime);
            Monitors.recordWorkerQueueSize("indexQueue", ((ThreadPoolExecutor) executorService).getQueue().size());
        } catch (Exception e) {
            logger.error("Failed to update task: {}", task.getTaskId(), e);
        }
    }

    @Override
    public CompletableFuture<Void> asyncCreateTask(Task task) {
        return CompletableFuture.runAsync(() -> createTask(task), executorService);
    }

    @Override
    public CompletableFuture<Void> asyncUpdateTask(Task task) {
        return CompletableFuture.runAsync(() -> updateTask(task), executorService);
    }

    @Override
    public void addTaskExecutionLogs(List<TaskExecLog> taskExecLogs) {
        if (taskExecLogs.isEmpty()) {
            return;
        }

        try {
            long startTime = Instant.now().toEpochMilli();
            BulkRequestBuilderWrapper bulkRequestBuilder = new BulkRequestBuilderWrapper(elasticSearchClient.prepareBulk());
            for (TaskExecLog log : taskExecLogs) {
                IndexRequest request = new IndexRequest(logIndexName, LOG_DOC_TYPE);
                request.source(objectMapper.writeValueAsBytes(log), XContentType.JSON);
                bulkRequestBuilder.add(request);
            }
            new RetryUtil<BulkResponse>().retryOnException(
                () -> bulkRequestBuilder.execute().actionGet(5, TimeUnit.SECONDS),
                null,
                BulkResponse::hasFailures,
                RETRY_COUNT,
                "Indexing task execution logs",
                "addTaskExecutionLogs"
            );
            long endTime = Instant.now().toEpochMilli();
            logger.debug("Time taken {} for indexing taskExecutionLogs", endTime - startTime);
            Monitors.recordESIndexTime("index_task_execution_logs", LOG_DOC_TYPE, endTime - startTime);
            Monitors.recordWorkerQueueSize("logQueue", ((ThreadPoolExecutor) logExecutorService).getQueue().size());
        } catch (Exception e) {
            List<String> taskIds = taskExecLogs.stream()
                .map(TaskExecLog::getTaskId)
                .collect(Collectors.toList());
            logger.error("Failed to index task execution logs for tasks: {}", taskIds, e);
        }
    }

    @Override
    public CompletableFuture<Void> asyncAddTaskExecutionLogs(List<TaskExecLog> logs) {
        return CompletableFuture.runAsync(() -> addTaskExecutionLogs(logs), logExecutorService);
    }

    @Override
    public List<TaskExecLog> getTaskExecutionLogs(String taskId) {
        try {
            Expression expression = Expression.fromString("taskId='" + taskId + "'");
            QueryBuilder queryBuilder = expression.getFilterBuilder();

            BoolQueryBuilder filterQuery = QueryBuilders.boolQuery().must(queryBuilder);
            QueryStringQueryBuilder stringQuery = QueryBuilders.queryStringQuery("*");
            BoolQueryBuilder fq = QueryBuilders.boolQuery().must(stringQuery).must(filterQuery);

            FieldSortBuilder sortBuilder = SortBuilders.fieldSort("createdTime")
                .order(SortOrder.ASC);
            final SearchRequestBuilder srb = elasticSearchClient.prepareSearch(logIndexPrefix + "*")
                .setQuery(fq)
                .setTypes(LOG_DOC_TYPE)
                .addSort(sortBuilder)
                .setSize(config.getElasticSearchTasklogLimit());
            
            SearchResponse response = srb.execute().actionGet();

            return Arrays.stream(response.getHits().getHits())
                .map(hit -> {
                    String source = hit.getSourceAsString();
                    try {
                        return objectMapper.readValue(source, TaskExecLog.class);
                    } catch (IOException e) {
                        logger.error("exception deserializing taskExecLog: {}", source);
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        } catch (Exception e) {
            logger.error("Failed to get task execution logs for task: {}", taskId, e);
        }

        return null;
    }

    @Override
    public void addMessage(String queue, Message message) {
        try {
            long startTime = Instant.now().toEpochMilli();
            Map<String, Object> doc = new HashMap<>();
            doc.put("messageId", message.getId());
            doc.put("payload", message.getPayload());
            doc.put("queue", queue);
            doc.put("created", System.currentTimeMillis());

            UpdateRequest req = new UpdateRequest(logIndexName, MSG_DOC_TYPE, message.getId());
            req.doc(doc, XContentType.JSON);
            req.upsert(doc, XContentType.JSON);
            indexObject(req, MSG_DOC_TYPE);
            long endTime = Instant.now().toEpochMilli();
            logger.debug("Time taken {} for  indexing message: {}", endTime - startTime, message.getId());
            Monitors.recordESIndexTime("add_message", MSG_DOC_TYPE, endTime - startTime);
        } catch (Exception e) {
            logger.error("Failed to index message: {}", message.getId(), e);
        }
    }

    @Override
    public CompletableFuture<Void> asyncAddMessage(String queue, Message message) {
        return CompletableFuture.runAsync(() -> addMessage(queue, message), executorService);
    }

    @Override
    public void addEventExecution(EventExecution eventExecution) {
        try {
            long startTime = Instant.now().toEpochMilli();
            byte[] doc = objectMapper.writeValueAsBytes(eventExecution);
            String id =
                eventExecution.getName() + "." + eventExecution.getEvent() + "." + eventExecution
                    .getMessageId() + "." + eventExecution.getId();

            UpdateRequest req = new UpdateRequest(logIndexName, EVENT_DOC_TYPE, id);
            req.doc(doc, XContentType.JSON);
            req.upsert(doc, XContentType.JSON);
            indexObject(req, EVENT_DOC_TYPE);
            long endTime = Instant.now().toEpochMilli();
            logger.debug("Time taken {} for indexing event execution: {}", endTime - startTime, eventExecution.getId());
            Monitors.recordESIndexTime("add_event_execution", EVENT_DOC_TYPE, endTime - startTime);
            Monitors.recordWorkerQueueSize("logQueue", ((ThreadPoolExecutor) logExecutorService).getQueue().size());
        } catch (Exception e) {
            logger.error("Failed to index event execution: {}", eventExecution.getId(), e);
        }
    }

    @Override
    public CompletableFuture<Void> asyncAddEventExecution(EventExecution eventExecution) {
        return CompletableFuture.runAsync(() -> addEventExecution(eventExecution), logExecutorService);
    }

    private void indexObject(IndexRequest req, String docType) {
        if (bulkRequests.get(docType) == null) {
            bulkRequests.put(docType, new BulkRequests<>(System.currentTimeMillis(), new BulkRequestBuilderWrapper(elasticSearchClient.prepareBulk())));
        }
        bulkRequests.get(docType).getBulkRequest().add(req);
        if (bulkRequests.get(docType).getBulkRequest().numberOfActions() >= this.indexBatchSize) {
            indexBulkRequest(docType);
        }
    }

    private void indexObject(UpdateRequest req, String docType) {
        if (bulkRequests.get(docType) == null) {
            bulkRequests.put(docType, new BulkRequests<>(
                    System.currentTimeMillis(),
                    new BulkRequestBuilderWrapper(elasticSearchClient.prepareBulk())));
        }
        bulkRequests.get(docType).getBulkRequest().add(req);
        if (bulkRequests.get(docType).getBulkRequest().numberOfActions() >= this.indexBatchSize) {
            indexBulkRequest(docType);
        }
    }

    private void updateObject(UpdateRequest req, String docType, final Long createdTime) {
        if (bulkUpdateRequests.get(docType) == null) {
            bulkUpdateRequests.put(docType, new BulkRequests<>(System.currentTimeMillis(), new BulkUpdateRequestsWrapper()));
        }
        bulkUpdateRequests.get(docType).getBulkRequest().addRequest(new RequestWrapper<>(req, createdTime));
        if (bulkUpdateRequests.get(docType).getBulkRequest().numberOfActions() >= this.indexBatchSize) {
            updateBulkRequest(docType);
        }
    }

    private synchronized void indexBulkRequest(String docType) {
        if (bulkRequests.get(docType).getBulkRequest() != null && bulkRequests.get(docType).getBulkRequest().numberOfActions() > 0) {
            indexWithRetry(bulkRequests.get(docType).getBulkRequest(), docType);
            bulkRequests.put(docType, new BulkRequests<>(
                    System.currentTimeMillis(),
                    new BulkRequestBuilderWrapper(elasticSearchClient.prepareBulk())));
        }
    }

    private synchronized void updateBulkRequest(String docType) {
        if (bulkRequests.get(docType).getBulkRequest().numberOfActions() > 0) {
            // Flush create requests of a same type before update requests as updates will if the corresponding create request is pending
            indexWithRetry(bulkRequests.get(docType).getBulkRequest(), docType);
        }

        if (bulkUpdateRequests.get(docType).getBulkRequest() != null && bulkUpdateRequests.get(docType).getBulkRequest().numberOfActions() > 0) {
            updateWithRetry(bulkUpdateRequests.get(docType).getBulkRequest(), docType);
            bulkUpdateRequests.put(docType, new BulkRequests<>(System.currentTimeMillis(), new BulkUpdateRequestsWrapper()));
        }
    }

    private void indexWithRetry(BulkRequestBuilderWrapper request, String docType) {
        try {
            long startTime = Instant.now().toEpochMilli();
            new RetryUtil<BulkResponse>().retryOnException(
                    () -> request.execute().actionGet(5, TimeUnit.SECONDS),
                    null,
                    BulkResponse::hasFailures,
                    RETRY_COUNT,
                    "Bulk Indexing "+ docType,
                    "indexObject",
                    //TODO: Remove after debugging
                    Collections.emptyList(),
                    (response) -> logger.error("Failed to index {} for ids : {}", docType, Arrays.stream(response.getItems())
                            .filter(BulkItemResponse::isFailed)
                            .map(BulkItemResponse::getId)
                            .collect(Collectors.joining(",")))
            );
            long endTime = Instant.now().toEpochMilli();
            logger.debug("Time taken {} for indexing object of type: {}", endTime - startTime, docType);
            Monitors.recordESIndexTime("index_object", docType, endTime - startTime);
        } catch (Exception e) {
            Monitors.error(className, "index");
            logger.error("Failed to index object of type: {}", docType, e);
        }
    }

    private void updateWithRetry(final BulkUpdateRequestsWrapper request, String docType) {
        try {
            long startTime = Instant.now().toEpochMilli();
            new RetryUtil<BulkResponse>().retryOnException(
                    () -> elasticSearchClient.bulk(request.constructBulkRequest()).actionGet(5, TimeUnit.SECONDS),
                    null,
                    BulkResponse::hasFailures,
                    RETRY_COUNT,
                    "Bulk updating "+ docType,
                    "updateObject",
                    retryListenerProvider.getBulkUpdateRequestRetryListeners(request),
                    //TODO: Remove after debugging
                    (response) -> logger.error("Failed to index {} for ids : {}", docType, Arrays.stream(response.getItems())
                            .filter(BulkItemResponse::isFailed)
                            .map(BulkItemResponse::getId)
                            .collect(Collectors.joining(",")))
            );
            long endTime = Instant.now().toEpochMilli();
            logger.debug("Time taken {} for updating object of type: {}", endTime - startTime, docType);
            Monitors.recordESIndexTime("update_object", docType, endTime - startTime);
        } catch (Exception e) {
            Monitors.error(className, "updateObject");
            logger.error("Failed to update object of type: {}", docType, e);
        }
    }


    @Override
    public SearchResult<String> searchWorkflows(String query, String freeText, int start, int count, List<String> sort) {
        return search(indexManager.getIndexNameProvider().getSearchRequestIndexName(), query, start, count, sort, freeText, WORKFLOW_DOC_TYPE);
    }

    @Override
    public SearchResult<String> searchTasks(String query, String freeText, int start, int count, List<String> sort) {
        return search(indexManager.getIndexNameProvider().getSearchRequestIndexName(), query, start, count, sort, freeText, TASK_DOC_TYPE);
    }

    @Override
    public void removeWorkflow(String workflowId) {
        try {
            long startTime = Instant.now().toEpochMilli();
            DeleteRequest request = new DeleteRequest(indexName, WORKFLOW_DOC_TYPE, workflowId);
            Predicate<DeleteResponse> retryCondition = deleteResponse -> deleteResponse == null || deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND;

            IndexRequestWrapper<DeleteRequest, DeleteResponse> indexRequestWrapper = new IndexRequestWrapper<>(
                    request,
                    deleteRequest -> elasticSearchClient.delete(request).actionGet(),
                    null);

            DeleteResponse response = new RetryUtil<DeleteResponse>().retryOnException(
                    indexRequestWrapper,
                    null,
                    retryCondition::test,
                    RETRY_COUNT,
                    "Deleting workflow document: " + workflowId, "getWorkflow",
                    retryListenerProvider.getDeleteRequestRetryListener(indexRequestWrapper));

            if (response.getResult() == DocWriteResponse.Result.DELETED) {
                logger.error("Index removal failed - document not found by id: {}", workflowId);
            }
            long endTime = Instant.now().toEpochMilli();
            logger.debug("Time taken {} for removing workflow: {}", endTime - startTime, workflowId);
            Monitors.recordESIndexTime("remove_workflow", WORKFLOW_DOC_TYPE, endTime - startTime);
            Monitors.recordWorkerQueueSize("indexQueue", ((ThreadPoolExecutor) executorService).getQueue().size());
        } catch (Exception e) {
            logger.error("Failed to remove workflow {} from index", workflowId, e);
            Monitors.error(className, "remove");
        }
    }

    @Override
    public CompletableFuture<Void> asyncRemoveWorkflow(String workflowId) {
        return CompletableFuture.runAsync(() -> removeWorkflow(workflowId), executorService);
    }

    @Override
    public void updateWorkflow(Workflow workflow) {
        try {
            long startTime = Instant.now().toEpochMilli();
            String id = workflow.getWorkflowId();
            WorkflowSummary summary = new WorkflowSummary(workflow);
            byte[] doc = objectMapper.writeValueAsBytes(summary);

            UpdateRequest request = new UpdateRequest(indexName, WORKFLOW_DOC_TYPE, id);
            request.doc(doc, XContentType.JSON);
            request.retryOnConflict(5);

            IndexRequestWrapper<UpdateRequest, UpdateResponse> requestWrapper = new IndexRequestWrapper<>(request,
                    (updateRequest) -> elasticSearchClient.update(updateRequest).actionGet(),
                    workflow.getCreateTime());

            new RetryUtil<UpdateResponse>().retryOnException(
                    requestWrapper,
                    null,
                    null,
                    RETRY_COUNT,
                    "Updating workflow document: " + workflow.getWorkflowId(),
                    "updateWorkflow",
                    retryListenerProvider.getUpdateRequestRetryListeners(requestWrapper)
            );

            long endTime = Instant.now().toEpochMilli();
            logger.debug("Time taken {} for updating workflow: {}", endTime - startTime, workflow.getWorkflowId());
            Monitors.recordESIndexTime("update_workflow", WORKFLOW_DOC_TYPE, endTime - startTime);
            Monitors.recordWorkerQueueSize("indexQueue", ((ThreadPoolExecutor) executorService).getQueue().size());
        } catch (Exception e) {
            Monitors.error(className, "updateWorkflow");
            logger.error("Failed to update workflow: {}", workflow.getWorkflowId(), e);
        }
    }

    @Override
    public void updateWorkflow(Workflow workflow, String[] keys, Object[] values) {
        if (keys.length != values.length) {
            throw new ApplicationException(Code.INVALID_INPUT,
                "Number of keys and values do not match");
        }

        long startTime = Instant.now().toEpochMilli();
        UpdateRequest request = new UpdateRequest(indexName, WORKFLOW_DOC_TYPE, workflow.getWorkflowId());
        Map<String, Object> source = IntStream.range(0, keys.length)
            .boxed()
            .collect(Collectors.toMap(i -> keys[i], i -> values[i]));
        request.doc(source);
        logger.debug("Updating workflow {} in elasticsearch index: {}", workflow.getWorkflowId(), indexName);
        IndexRequestWrapper<UpdateRequest, UpdateResponse> requestWrapper = new IndexRequestWrapper<>(request,
                (updateRequest) -> elasticSearchClient.update(updateRequest).actionGet(),
                workflow.getCreateTime());

        new RetryUtil<UpdateResponse>().retryOnException(
                requestWrapper,
                null,
                null,
                RETRY_COUNT,
                "Updating workflow document: " + workflow.getWorkflowId(),
                "updateWorkflow",
                retryListenerProvider.getUpdateRequestRetryListeners(requestWrapper)
        );

        long endTime = Instant.now().toEpochMilli();
        logger.debug("Time taken {} for updating workflow: {}", endTime - startTime, workflow.getWorkflowId());
        Monitors.recordESIndexTime("update_workflow", WORKFLOW_DOC_TYPE, endTime - startTime);
        Monitors.recordWorkerQueueSize("indexQueue", ((ThreadPoolExecutor) executorService).getQueue().size());
    }

    @Override
    public CompletableFuture<Void> asyncUpdateWorkflow(Workflow workflow, String[] keys,
        Object[] values) {
        return CompletableFuture.runAsync(() -> updateWorkflow(workflow, keys, values), executorService);
    }

    @Override
    public String get(String workflowInstanceId, String fieldToGet) {
        GetRequest request = new GetRequest(indexName, WORKFLOW_DOC_TYPE, workflowInstanceId)
            .fetchSourceContext(
                new FetchSourceContext(true, new String[]{fieldToGet}, Strings.EMPTY_ARRAY));

        Predicate<GetResponse> retryCondition = requestResponse -> requestResponse == null || !requestResponse.isExists();

        IndexRequestWrapper<GetRequest, GetResponse> indexRequestWrapper = new IndexRequestWrapper<>(
                request,
                getRequest -> elasticSearchClient.get(getRequest).actionGet(),
                null);

        GetResponse response = new RetryUtil<GetResponse>().retryOnException(
                indexRequestWrapper,
                null,
                retryCondition::test,
                RETRY_COUNT,
                "Getting workflow document: " + workflowInstanceId,
                "getWorkflow",
                retryListenerProvider.getLookUpRequestRetryListener(indexRequestWrapper));

        if (response.isExists()) {
            Map<String, Object> sourceAsMap = response.getSourceAsMap();
            if (sourceAsMap.containsKey(fieldToGet)) {
                return sourceAsMap.get(fieldToGet).toString();
            }
        }

        logger.info("Unable to find Workflow: {} in ElasticSearch index: {}.", workflowInstanceId, indexName);
        return null;
    }

    private SearchResult<String> search(String indexName, String structuredQuery, int start, int size,
        List<String> sortOptions, String freeTextQuery, String docType) {
        try {
            QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
            if (StringUtils.isNotEmpty(structuredQuery)) {
                Expression expression = Expression.fromString(structuredQuery);
                queryBuilder = expression.getFilterBuilder();
            }

            BoolQueryBuilder filterQuery = QueryBuilders.boolQuery().must(queryBuilder);
            QueryStringQueryBuilder stringQuery = QueryBuilders.queryStringQuery(freeTextQuery);
            BoolQueryBuilder fq = QueryBuilders.boolQuery().must(stringQuery).must(filterQuery);
            final SearchRequestBuilder srb = elasticSearchClient.prepareSearch(indexName)
                    .setQuery(fq)
                    .setTypes(docType)
                    .storedFields("_id")
                    .setFrom(start)
                    .setSize(size);

            if (sortOptions != null) {
                sortOptions.forEach(sortOption -> addSortOptionToSearchRequest(srb, sortOption));
            }

            SearchResponse response = srb.get();

            LinkedList<String> result = StreamSupport.stream(response.getHits().spliterator(), false)
                    .map(SearchHit::getId)
                    .collect(Collectors.toCollection(LinkedList::new));
            long count = response.getHits().getTotalHits();

            return new SearchResult<>(count, result);
        } catch (ParserException e) {
            String errorMsg = String.format("Error performing search on index:%s with docType:%s", indexName, docType);
            logger.error(errorMsg);
            throw new ApplicationException(Code.BACKEND_ERROR, errorMsg, e);
        }
    }

    private void addSortOptionToSearchRequest(SearchRequestBuilder searchRequestBuilder,
        String sortOption) {
        SortOrder order = SortOrder.ASC;
        String field = sortOption;
        int indx = sortOption.indexOf(':');
        if (indx > 0) {    // Can't be 0, need the field name at-least
            field = sortOption.substring(0, indx);
            order = SortOrder.valueOf(sortOption.substring(indx + 1));
        }
        searchRequestBuilder.addSort(field, order);
    }

    @Override
    public List<String> searchArchivableWorkflows(String indexName, long archiveTtlDays) {
        QueryBuilder q = QueryBuilders.boolQuery()
            .must(QueryBuilders.rangeQuery("endTime").lt(LocalDate.now(ZoneOffset.UTC).minusDays(archiveTtlDays).toString()).gte(LocalDate.now(ZoneOffset.UTC).minusDays(archiveTtlDays).minusDays(1).toString()))
            .should(QueryBuilders.termQuery("status", "COMPLETED"))
            .should(QueryBuilders.termQuery("status", "FAILED"))
            .should(QueryBuilders.termQuery("status", "TIMED_OUT"))
            .should(QueryBuilders.termQuery("status", "TERMINATED"))
            .mustNot(QueryBuilders.existsQuery("archived"))
            .minimumShouldMatch(1);
        SearchRequestBuilder s = elasticSearchClient.prepareSearch(indexName)
            .setTypes("workflow")
            .setQuery(q)
            .addSort("endTime", SortOrder.ASC)
            .setSize(archiveSearchBatchSize);

        SearchResponse response = s.execute().actionGet();

        SearchHits hits = response.getHits();
        logger.info("Archive search totalHits - {}", hits.getTotalHits());

        return Arrays.stream(hits.getHits())
            .map(SearchHit::getId)
            .collect(Collectors.toCollection(LinkedList::new));
    }

    @Override
    public List<String> searchRecentRunningWorkflows(int lastModifiedHoursAgoFrom,
        int lastModifiedHoursAgoTo) {
        DateTime dateTime = new DateTime();
        QueryBuilder q = QueryBuilders.boolQuery()
            .must(QueryBuilders.rangeQuery("updateTime")
                .gt(dateTime.minusHours(lastModifiedHoursAgoFrom)))
            .must(QueryBuilders.rangeQuery("updateTime")
                .lt(dateTime.minusHours(lastModifiedHoursAgoTo)))
            .must(QueryBuilders.termQuery("status", "RUNNING"));

        SearchRequestBuilder s = elasticSearchClient.prepareSearch(indexManager.getIndexNameProvider().getSearchRequestIndexName())
            .setTypes("workflow")
            .setQuery(q)
            .setSize(5000)
            .addSort("updateTime", SortOrder.ASC);

        SearchResponse response = s.execute().actionGet();
        return StreamSupport.stream(response.getHits().spliterator(), false)
            .map(SearchHit::getId)
            .collect(Collectors.toCollection(LinkedList::new));
    }

    @Override
    public List<Message> getMessages(String queue) {
        try {
            Expression expression = Expression.fromString("queue='" + queue + "'");
            QueryBuilder queryBuilder = expression.getFilterBuilder();

            BoolQueryBuilder filterQuery = QueryBuilders.boolQuery().must(queryBuilder);
            QueryStringQueryBuilder stringQuery = QueryBuilders.queryStringQuery("*");
            BoolQueryBuilder fq = QueryBuilders.boolQuery().must(stringQuery).must(filterQuery);

            final SearchRequestBuilder srb = elasticSearchClient.prepareSearch(logIndexPrefix + "*")
                    .setQuery(fq)
                    .setTypes(MSG_DOC_TYPE)
                    .addSort(SortBuilders.fieldSort("created").order(SortOrder.ASC));

            return mapGetMessagesResponse(srb.execute().actionGet());
        } catch (Exception e) {
            String errorMsg = String.format("Failed to get messages for queue: %s", queue);
            logger.error(errorMsg, e);
            throw new ApplicationException(Code.BACKEND_ERROR, errorMsg, e);
        }
    }

    private List<Message> mapGetMessagesResponse(SearchResponse response) throws IOException {
        SearchHit[] hits = response.getHits().getHits();
        TypeFactory factory = TypeFactory.defaultInstance();
        MapType type = factory.constructMapType(HashMap.class, String.class, String.class);
        List<Message> messages = new ArrayList<>(hits.length);
        for (SearchHit hit : hits) {
            String source = hit.getSourceAsString();
            Map<String, String> mapSource = objectMapper.readValue(source, type);
            Message msg = new Message(mapSource.get("messageId"), mapSource.get("payload"), null);
            messages.add(msg);
        }
        return messages;
    }

    @Override
    public List<EventExecution> getEventExecutions(String event) {
        try {
            Expression expression = Expression.fromString("event='" + event + "'");
            QueryBuilder queryBuilder = expression.getFilterBuilder();

            BoolQueryBuilder filterQuery = QueryBuilders.boolQuery().must(queryBuilder);
            QueryStringQueryBuilder stringQuery = QueryBuilders.queryStringQuery("*");
            BoolQueryBuilder fq = QueryBuilders.boolQuery().must(stringQuery).must(filterQuery);

            final SearchRequestBuilder srb = elasticSearchClient.prepareSearch(logIndexPrefix + "*")
                    .setQuery(fq).setTypes(EVENT_DOC_TYPE)
                    .addSort(SortBuilders.fieldSort("created")
                            .order(SortOrder.ASC));

            return mapEventExecutionsResponse(srb.execute().actionGet());
        } catch (Exception e) {
            String errorMsg = String.format("Failed to get executions for event: %s", event);
            logger.error(errorMsg, e);
            throw new ApplicationException(Code.BACKEND_ERROR, errorMsg, e);
        }
    }

    private List<EventExecution> mapEventExecutionsResponse(SearchResponse response) throws IOException {
        SearchHit[] hits = response.getHits().getHits();
        List<EventExecution> executions = new ArrayList<>(hits.length);
        for (SearchHit hit : hits) {
            String source = hit.getSourceAsString();
            EventExecution tel = objectMapper.readValue(source, EventExecution.class);
            executions.add(tel);
        }
        return executions;
    }

    /**
     * Flush the buffers if bulk requests have not been indexed for the past {@link ElasticSearchConfiguration#ELASTIC_SEARCH_ASYNC_BUFFER_FLUSH_TIMEOUT_PROPERTY_NAME} seconds
     * This is to prevent data loss in case the instance is terminated, while the buffer still holds documents to be indexed.
     */
    private void flushBulkRequests() {
        bulkRequests.entrySet().stream()
            .filter(entry -> (System.currentTimeMillis() - entry.getValue().getLastFlushTime()) >= asyncBufferFlushTimeout * 1000)
            .filter(entry -> entry.getValue().getBulkRequest() != null && entry.getValue().getBulkRequest().numberOfActions() > 0)
            .forEach(entry -> {
                logger.debug("Flushing bulk request buffer for type {}, size: {}", entry.getKey(), entry.getValue().getBulkRequest().numberOfActions());
                indexBulkRequest(entry.getKey());
            });

        // Perform update request after the create requests
        bulkUpdateRequests.entrySet().stream()
                .filter(entry -> (System.currentTimeMillis() - entry.getValue().getLastFlushTime()) >= asyncBufferFlushTimeout * 1000)
                .filter(entry -> entry.getValue().getBulkRequest() != null && entry.getValue().getBulkRequest().numberOfActions() > 0)
                .forEach(entry -> {
                    logger.debug("Flushing bulk update request buffer for type {}, size: {}",
                            entry.getKey(),
                            entry.getValue().getBulkRequest().numberOfActions());
                    updateBulkRequest(entry.getKey());
                });
    }
}
