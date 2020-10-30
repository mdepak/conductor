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

import java.util.Collections;
import java.util.List;

public class DefaultRetryListenerProvider implements RetryListenerProvider {

    @Override
    public List<RetryListener> getUpdateRequestRetryListeners(IndexRequestWrapper<UpdateRequest, UpdateResponse> updateRequest) {
        return Collections.emptyList();
    }

    @Override
    public List<RetryListener> getLookUpRequestRetryListener(IndexRequestWrapper<GetRequest, GetResponse> getRequest) {
        return Collections.emptyList();
    }

    @Override
    public List<RetryListener> getDeleteRequestRetryListener(IndexRequestWrapper<DeleteRequest, DeleteResponse> deleteRequest) {
        return Collections.emptyList();
    }

    @Override
    public List<RetryListener> getBulkUpdateRequestRetryListeners(BulkUpdateRequestsWrapper bulkUpdateRequest) {
        return Collections.emptyList();
    }
}
