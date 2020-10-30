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
package com.netflix.conductor.elasticsearch.rollover;

import java.util.List;

public interface IndexNameProvider {

    /**
     * @return the name of the index to create
     */
    String getIndexNameToCreate();

    /**
     *
     * @param attempt retry attempt count
     * @param createdTime creation time of document
     * @return the index name to write the document based on the retry attempt & creation time
     */
    String getWriteIndexName(int attempt, long createdTime);

    /**
     *
     * @param attempt   retry attempt count
     * @return the index name to get the document based on the retry attempt
     */
    String getLookupRequestIndexName(int attempt);

    /**
     *
     * @return the index name to search documents
     */
    String getSearchRequestIndexName();

    /**
     *
     * @param indexList updates the name provider with new set of indices
     */
    default void updateIndices(List<Index> indexList){
    }
}
