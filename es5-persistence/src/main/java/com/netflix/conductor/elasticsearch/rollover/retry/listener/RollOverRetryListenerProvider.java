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

import com.github.rholder.retry.RetryListener;
import com.google.inject.Inject;
import com.netflix.conductor.dao.es5.index.BulkUpdateRequestsWrapper;
import com.netflix.conductor.dao.es5.index.IndexRequestWrapper;
import com.netflix.conductor.elasticsearch.rollover.IndexNameProvider;
import com.netflix.conductor.elasticsearch.rollover.retry.validator.DocumentMissingValidator;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;

import java.util.Collections;
import java.util.List;

public class RollOverRetryListenerProvider implements RetryListenerProvider {

    private final IndexNameProvider indexNameProvider;
    private final DocumentMissingValidator documentMissingValidator;

    @Inject
    RollOverRetryListenerProvider(IndexNameProvider indexNameProvider, DocumentMissingValidator documentMissingValidator) {
        this.indexNameProvider = indexNameProvider;
        this.documentMissingValidator = documentMissingValidator;
    }

    @Override
    public List<RetryListener> getUpdateRequestRetryListeners(IndexRequestWrapper<UpdateRequest, UpdateResponse> updateRequest) {
        return Collections.singletonList(new RolloverUpdateListener(updateRequest, indexNameProvider, documentMissingValidator));
    }

    @Override
    public List<RetryListener> getLookUpRequestRetryListener(IndexRequestWrapper<GetRequest, GetResponse> getRequest) {
        return Collections.singletonList(new LookUpRequestRetryListener(getRequest, indexNameProvider));
    }

    @Override
    public List<RetryListener> getDeleteRequestRetryListener(IndexRequestWrapper<DeleteRequest, DeleteResponse> deleteRequest) {
        return Collections.singletonList(new DeleteRequestRetryListener(deleteRequest, indexNameProvider));
    }

    @Override
    public List<RetryListener> getBulkUpdateRequestRetryListeners(BulkUpdateRequestsWrapper bulkUpdateRequest) {
        return Collections.singletonList(new RolloverBulkUpdateListener(bulkUpdateRequest, indexNameProvider, documentMissingValidator));
    }
}
