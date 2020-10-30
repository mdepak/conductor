package com.netflix.conductor.elasticsearch.rollover.retry.validator;

import com.github.rholder.retry.Attempt;
import org.elasticsearch.action.bulk.BulkItemResponse;

import java.util.function.Predicate;

public class DocumentMissingValidatorRestDAOV5 implements DocumentMissingValidator {
    @Override
    public Predicate<Attempt> getUpdateDocumentMissingValidator() {
        return attempt -> !attempt.hasResult() && attempt.getExceptionCause()!= null && attempt.getExceptionCause().getLocalizedMessage().contains("document_missing_exception");
    }

    @Override
    public Predicate<BulkItemResponse> getBulkUpdateDocumentMissingValidator() {
        return bulkResponse -> bulkResponse.isFailed() && bulkResponse.getFailure().getMessage().contains("document_missing_exception");
    }
}
