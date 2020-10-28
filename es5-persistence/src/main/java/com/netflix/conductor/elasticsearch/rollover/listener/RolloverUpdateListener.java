package com.netflix.conductor.elasticsearch.rollover.listener;

import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryListener;
import com.netflix.conductor.dao.es5.index.IndexRequestWrapper;
import com.netflix.conductor.elasticsearch.rollover.IndexNameProvider;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RolloverUpdateListener implements RetryListener {

    private static final Logger logger = LoggerFactory.getLogger(RolloverUpdateListener.class);

    private final IndexRequestWrapper<UpdateRequest, UpdateResponse> updateRequest;
    private final IndexNameProvider indexNameProvider;

    public RolloverUpdateListener(IndexRequestWrapper<UpdateRequest, UpdateResponse> updateRequest, IndexNameProvider indexNameProvider) {
        this.updateRequest = updateRequest;
        this.indexNameProvider = indexNameProvider;
    }

    @Override
    public <V> void onRetry(Attempt<V> attempt) {
        try {
            if (!attempt.hasResult() && attempt.getExceptionCause()!= null && attempt.getExceptionCause().getLocalizedMessage().contains("document_missing_exception")) {
                UpdateRequest oldRequest = updateRequest.getRequest();
                String newIndexName = indexNameProvider.getWriteIndexName((int) attempt.getAttemptNumber(), updateRequest.getCreatedTime());
                if (newIndexName == null) {
                    newIndexName = "conductor";
                }
                UpdateRequest request = new UpdateRequest(newIndexName, oldRequest.type(), oldRequest.id());
                request.doc(oldRequest.doc().sourceAsMap(), XContentType.JSON);
                updateRequest.setRequest(request);
                logger.info("Attempt : #{} Changing update index of {} request from {} to {}", attempt.getAttemptNumber(), oldRequest.id(), oldRequest.index(), request.index());
            }
        } catch (Exception ex) {
            logger.error("Exception in retry listener ", ex);
        }
    }
}
