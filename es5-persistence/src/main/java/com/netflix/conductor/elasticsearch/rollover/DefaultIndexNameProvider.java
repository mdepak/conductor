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

import com.netflix.conductor.elasticsearch.ElasticSearchConfiguration;

import javax.inject.Inject;

public class DefaultIndexNameProvider implements IndexNameProvider{

    private final String indexName;

    @Inject
    public DefaultIndexNameProvider(ElasticSearchConfiguration config){
        this.indexName = config.getIndexName();
    }

    @Override
    public String getIndexNameToCreate() {
        return indexName;
    }

    @Override
    public String getWriteIndexName(int attempt, long createdTime) {
        return indexName;
    }

    @Override
    public String getLookupRequestIndexName(int attempt) {
        return indexName;
    }

    @Override
    public String getSearchRequestIndexName() {
        return indexName;
    }
}
