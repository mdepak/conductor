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
