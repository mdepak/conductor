package com.netflix.conductor.elasticsearch.rollover.listener;

import com.github.rholder.retry.RetryListener;
import com.netflix.conductor.dao.es5.index.BulkUpdateRequestsWrapper;
import com.netflix.conductor.dao.es5.index.IndexRequestWrapper;
import com.netflix.conductor.elasticsearch.rollover.IndexNameProvider;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;

import javax.inject.Inject;
import java.util.Collections;
import java.util.List;

public class RollOverRetryListenerProvider implements RetryListenerProvider {

    private final IndexNameProvider indexNameProvider;

    @Inject
    RollOverRetryListenerProvider(IndexNameProvider indexNameProvider) {
        this.indexNameProvider = indexNameProvider;
    }

    @Override
    public List<RetryListener> getUpdateRequestRetryListeners(IndexRequestWrapper updateRequest) {
        return Collections.singletonList(new RolloverUpdateListener(updateRequest, indexNameProvider));
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
        return Collections.singletonList(new RolloverBulkUpdateListener(bulkUpdateRequest, indexNameProvider));
    }
}
