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
package com.netflix.conductor.elasticsearch.rollover.retry.listener;

import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryListener;
import com.netflix.conductor.dao.es5.index.BulkUpdateRequestsWrapper;
import com.netflix.conductor.elasticsearch.rollover.IndexNameProvider;
import com.netflix.conductor.elasticsearch.rollover.RequestWrapper;
import com.netflix.conductor.elasticsearch.rollover.retry.validator.DocumentMissingValidator;
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
    private final DocumentMissingValidator documentMissingValidator;

    /*Use this counter to indicate the retry count;
     Sometimes timeout exception happens and increasing the attempt count might lead to skipping the right index to update*/
    private int attemptCount = 0;

    public RolloverBulkUpdateListener(BulkUpdateRequestsWrapper updateRequest, IndexNameProvider indexNameProvider, DocumentMissingValidator documentMissingValidator) {
        this.updateRequest = updateRequest;
        this.indexNameProvider = indexNameProvider;
        this.documentMissingValidator = documentMissingValidator;
    }

    @Override
    public <V> void onRetry(Attempt<V> attempt) {
        try {

            List<RequestWrapper<UpdateRequest>> newRequests = Collections.synchronizedList(new ArrayList<>());

            if (attempt.hasResult()) {

                //Increase the attempt only when the bulk response is returned
                attemptCount++;

                BulkResponse bulkResponse = (BulkResponse) attempt.getResult();

                if (bulkResponse.getItems().length != updateRequest.getUpdateRequests().size()) {
                    logger.error("Mismatch in the number of requests and responses");
                }

                for (int idx = 0; idx < updateRequest.getUpdateRequests().size(); idx++) {

                    if (bulkResponse.getItems()[idx].isFailed()) {
                        RequestWrapper<UpdateRequest> requestWrapper = updateRequest.getUpdateRequests().get(idx);

                        if (documentMissingValidator.getBulkUpdateDocumentMissingValidator().test(bulkResponse.getItems()[idx])) {
                            UpdateRequest oldUpdateRequest = requestWrapper.getRequest();
                            String newIndexName = indexNameProvider.getWriteIndexName(attemptCount, requestWrapper.getCreatedTime());
                            if (newIndexName != null) {
                                newRequests.add(new RequestWrapper<>(copyUpdateRequest(oldUpdateRequest, newIndexName), requestWrapper.getCreatedTime()));
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
