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
import com.netflix.conductor.dao.es5.index.BulkUpdateRequestsWrapper;
import com.netflix.conductor.dao.es5.index.IndexRequestWrapper;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;

import java.util.List;

public interface RetryListenerProvider {

    /**
     *
     * @param updateRequest request to update a document
     *
     * @return the list of retry listener that will handle the exceptions during the UPDATE document request
     */
    List<RetryListener> getUpdateRequestRetryListeners(IndexRequestWrapper<UpdateRequest, UpdateResponse> updateRequest);

    /**
     *
     * @param getRequest request to get a document
     * @return the list of retry listener that will handle the exceptions during the GET document request
     */
    List<RetryListener> getLookUpRequestRetryListener(IndexRequestWrapper<GetRequest, GetResponse> getRequest);

    /**
     *
     * @param deleteRequest request to delete a document
     * @return the list of retry listener that will handle the exceptions during the DELETE document request
     */
    List<RetryListener> getDeleteRequestRetryListener(IndexRequestWrapper<DeleteRequest, DeleteResponse> deleteRequest);

    /**
     *
     * @param bulkUpdateRequest request to bulk update a list of documents
     * @return the list of retry listener that will handle the exceptions during the BULK_UPDATE documents request
     */
    List<RetryListener> getBulkUpdateRequestRetryListeners(BulkUpdateRequestsWrapper bulkUpdateRequest);
}
