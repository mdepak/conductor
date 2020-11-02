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
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LookUpRequestRetryListener implements RetryListener {
    private static final Logger logger = LoggerFactory.getLogger(LookUpRequestRetryListener.class);

    private final IndexRequestWrapper<GetRequest, GetResponse> indexRequestWrapper;
    private final IndexNameProvider indexNameProvider;
    private int retryAttempt = 0;

    public LookUpRequestRetryListener(IndexRequestWrapper<GetRequest, GetResponse> updateRequest, IndexNameProvider indexNameProvider) {
        this.indexRequestWrapper = updateRequest;
        this.indexNameProvider = indexNameProvider;

    }

    @Override
    public <V> void onRetry(Attempt<V> attempt) {
        try {
            if (attempt.hasException() || (attempt.hasResult())) {
                GetResponse response = (GetResponse) attempt.getResult();

                if (!response.isExists()) {
                    retryAttempt++;
                    String oldIndexName = indexRequestWrapper.getRequest().index();
                    String newIndexName = indexNameProvider.getLookupRequestIndexName((retryAttempt));
                    //TODO: Handle failure
                    if (newIndexName == null) {
                        newIndexName = "conductor";
                    }

                    indexRequestWrapper.getRequest().index(newIndexName);
                    logger.debug("Attempt : #{} Changed index of get {} request with id {} from {} to {}", attempt.getAttemptNumber(), indexRequestWrapper.getRequest().type(), indexRequestWrapper.getRequest().id(), oldIndexName, newIndexName);
                }
            }
        } catch (Exception ex) {
            logger.error("Exception in retry listener ", ex);
        }
    }
}
