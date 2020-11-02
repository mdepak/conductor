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
import com.netflix.conductor.dao.es5.index.IndexRequestWrapper;
import com.netflix.conductor.elasticsearch.rollover.IndexNameProvider;
import com.netflix.conductor.elasticsearch.rollover.retry.validator.DocumentMissingValidator;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RolloverUpdateListener implements RetryListener {

    private static final Logger logger = LoggerFactory.getLogger(RolloverUpdateListener.class);

    private final IndexRequestWrapper<UpdateRequest, UpdateResponse> updateRequest;
    private final IndexNameProvider indexNameProvider;
    private final DocumentMissingValidator documentMissingValidator;

    public RolloverUpdateListener(IndexRequestWrapper<UpdateRequest, UpdateResponse> updateRequest,
                                  IndexNameProvider indexNameProvider,
                                  DocumentMissingValidator documentMissingValidator) {
        this.updateRequest = updateRequest;
        this.indexNameProvider = indexNameProvider;
        this.documentMissingValidator = documentMissingValidator;
    }

    @Override
    public <V> void onRetry(Attempt<V> attempt) {
        try {
            if (documentMissingValidator.getUpdateDocumentMissingValidator().test(attempt)) {
                UpdateRequest oldRequest = updateRequest.getRequest();
                String newIndexName = indexNameProvider.getWriteIndexName((int) attempt.getAttemptNumber(), updateRequest.getCreatedTime());
                //TODO: Handle failure
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
