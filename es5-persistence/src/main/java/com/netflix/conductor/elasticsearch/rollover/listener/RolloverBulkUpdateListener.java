package com.netflix.conductor.elasticsearch.rollover.listener;

import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryListener;
import com.netflix.conductor.dao.es5.index.BulkUpdateRequestsWrapper;
import com.netflix.conductor.elasticsearch.rollover.IndexNameProvider;
import com.netflix.conductor.elasticsearch.rollover.RequestWrapper;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RolloverBulkUpdateListener implements RetryListener {

    private static final Logger logger = LoggerFactory.getLogger(RolloverBulkUpdateListener.class);

    private final BulkUpdateRequestsWrapper updateRequest;
    private final IndexNameProvider indexNameProvider;

    public RolloverBulkUpdateListener(BulkUpdateRequestsWrapper updateRequest, IndexNameProvider indexNameProvider) {
        this.updateRequest = updateRequest;
        this.indexNameProvider = indexNameProvider;
    }

    @Override
    public <V> void onRetry(Attempt<V> attempt) {
        try {

            List<RequestWrapper<UpdateRequest>> newRequests = Collections.synchronizedList(new ArrayList<>());

            if (attempt.hasResult()) {
                BulkResponse bulkResponse = (BulkResponse) attempt.getResult();

                if (bulkResponse.getItems().length != updateRequest.getUpdateRequests().size()) {
                    logger.error("Mismatch in the number of requests and responses");
                }

                for (int idx = 0; idx < updateRequest.getUpdateRequests().size(); idx++) {
                    BulkItemResponse.Failure requestFailure = bulkResponse.getItems()[idx].getFailure();
                    if (requestFailure != null) {
                        RequestWrapper<UpdateRequest> requestWrapper = updateRequest.getUpdateRequests().get(idx);

                        if (requestFailure.getMessage().contains("document_missing_exception")) {
                            UpdateRequest oldUpdateRequest = requestWrapper.getRequest();
                            String newIndexName = indexNameProvider.getWriteIndexName((int) attempt.getAttemptNumber(), requestWrapper.getCreatedTime());
                            if (newIndexName != null) {
                                newRequests.add(new RequestWrapper(copyUpdateRequest(oldUpdateRequest, newIndexName), requestWrapper.getCreatedTime()));
                            } else {
                                logger.warn("Failed to update document of type {} with id {}", oldUpdateRequest.type(), oldUpdateRequest.id());
                            }
                            logger.info("Attempt : #{} Changing update index of {} request from {} to {}",
                                    attempt.getAttemptNumber(), oldUpdateRequest.id(), oldUpdateRequest.index(), newIndexName);
                        } else {
                            // Other failures
                            newRequests.add(requestWrapper);
                        }
                    }
                }
            }

            if (!newRequests.isEmpty()) {
                updateRequest.setUpdateRequestWrapperPojos(newRequests);
            }
        } catch (Exception ex) {
            logger.error("Exception in retry listener ", ex);
        }
    }

    private UpdateRequest copyUpdateRequest(UpdateRequest oldRequest, String newIndexName) {
        UpdateRequest request = new UpdateRequest(newIndexName, oldRequest.type(), oldRequest.id());
        request.doc(oldRequest.doc().sourceAsMap(), XContentType.JSON);

        return request;
    }
}
