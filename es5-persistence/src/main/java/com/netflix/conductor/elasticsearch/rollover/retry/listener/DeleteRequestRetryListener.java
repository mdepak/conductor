package com.netflix.conductor.elasticsearch.rollover.retry.listener;

import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryListener;
import com.netflix.conductor.dao.es5.index.IndexRequestWrapper;
import com.netflix.conductor.elasticsearch.rollover.IndexNameProvider;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteRequestRetryListener implements RetryListener {

    private static final Logger logger = LoggerFactory.getLogger(DeleteRequestRetryListener.class);

    private final IndexRequestWrapper<DeleteRequest, DeleteResponse> deleteRequestWrapper;
    private final IndexNameProvider indexNameProvider;


    public DeleteRequestRetryListener(IndexRequestWrapper<DeleteRequest, DeleteResponse> deleteRequest, IndexNameProvider indexNameProvider) {
        this.deleteRequestWrapper = deleteRequest;
        this.indexNameProvider = indexNameProvider;
    }

    @Override
    public <V> void onRetry(Attempt<V> attempt) {
        try {
            if (attempt.hasException() || (attempt.hasResult())) {
                DeleteResponse response = (DeleteResponse) attempt.getResult();

                if (response.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                    String oldIndexName = deleteRequestWrapper.getRequest().index();
                    String newIndexName = indexNameProvider.getLookupRequestIndexName((int) attempt.getAttemptNumber());
                    if (newIndexName == null) {
                        newIndexName = "conductor";
                    }

                    deleteRequestWrapper.getRequest().index(newIndexName);
                    logger.debug("Attempt : #{} Changed index of get {} request with id {} from {} to {}",
                            attempt.getAttemptNumber(), deleteRequestWrapper.getRequest().type(),
                            deleteRequestWrapper.getRequest().id(),
                            oldIndexName, newIndexName);
                }
            }
        } catch (Exception ex) {
            logger.error("Exception in retry listener ", ex);
        }
    }
}
