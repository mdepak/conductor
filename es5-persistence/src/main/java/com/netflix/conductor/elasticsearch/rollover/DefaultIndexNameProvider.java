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
