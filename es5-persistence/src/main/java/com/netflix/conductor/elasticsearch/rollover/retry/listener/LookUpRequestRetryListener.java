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

    public LookUpRequestRetryListener(IndexRequestWrapper<GetRequest, GetResponse> updateRequest, IndexNameProvider indexNameProvider) {
        this.indexRequestWrapper = updateRequest;
        this.indexNameProvider = indexNameProvider;
    }

    @Override
    public <V> void onRetry(Attempt<V> attempt) {
        try {
            if (attempt.hasException() || (attempt.hasResult())) {
                GetResponse response  = (GetResponse) attempt.getResult();

                if(!response.isExists())
                {
                    String oldIndexName = indexRequestWrapper.getRequest().index();
                    String newIndexName = indexNameProvider.getLookupRequestIndexName((int) attempt.getAttemptNumber());
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
