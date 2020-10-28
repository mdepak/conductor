package com.netflix.conductor.dao.es5.index;

import com.google.common.base.Supplier;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

public class BulkUpdateRequestSupplier implements Supplier<BulkResponse> {
    private BulkUpdateRequestsWrapper bulkRequest;
    private final RestHighLevelClient elasticSearchClient;

    public BulkUpdateRequestsWrapper getBulkRequest() {
        return bulkRequest;
    }

    public void setBulkRequest(BulkUpdateRequestsWrapper bulkRequest) {
        this.bulkRequest = bulkRequest;
    }

    BulkUpdateRequestSupplier(BulkUpdateRequestsWrapper bulkRequest, RestHighLevelClient elasticSearchClient) {
        this.bulkRequest = bulkRequest;
        this.elasticSearchClient = elasticSearchClient;
    }

    @Override
    public BulkResponse get() {
        try {
            return elasticSearchClient.bulk(bulkRequest.constructBulkRequest());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
