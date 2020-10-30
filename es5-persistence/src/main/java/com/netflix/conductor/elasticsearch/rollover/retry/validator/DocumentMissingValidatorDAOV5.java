package com.netflix.conductor.elasticsearch.rollover.retry.validator;

import com.github.rholder.retry.Attempt;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.index.engine.DocumentMissingException;

import java.util.function.Predicate;

public class DocumentMissingValidatorDAOV5 implements DocumentMissingValidator{
    @Override
    public Predicate<Attempt> getUpdateDocumentMissingValidator() {
        return attempt -> !attempt.hasResult() && attempt.getExceptionCause() instanceof DocumentMissingException;
    }

    @Override
    public Predicate<BulkItemResponse> getBulkUpdateDocumentMissingValidator() {
        return bulkItemResponse -> bulkItemResponse.isFailed() && bulkItemResponse.getFailure().getCause() instanceof DocumentMissingException;
    }
}
