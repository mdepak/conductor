package com.netflix.conductor.elasticsearch.rollover.retry.validator;

import com.github.rholder.retry.Attempt;
import org.elasticsearch.action.bulk.BulkItemResponse;

import java.util.function.Predicate;

public interface DocumentMissingValidator {
    Predicate<Attempt> getUpdateDocumentMissingValidator();

    Predicate<BulkItemResponse> getBulkUpdateDocumentMissingValidator();
}
