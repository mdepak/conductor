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
