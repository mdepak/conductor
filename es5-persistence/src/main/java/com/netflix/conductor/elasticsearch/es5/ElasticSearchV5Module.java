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
package com.netflix.conductor.elasticsearch.es5;

import com.google.inject.AbstractModule;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.es5.index.ElasticSearchDAOV5;
import com.netflix.conductor.dao.es5.index.ElasticSearchRestDAOV5;
import com.netflix.conductor.elasticsearch.ElasticSearchConfiguration;
import com.netflix.conductor.elasticsearch.EmbeddedElasticSearchProvider;
import com.netflix.conductor.elasticsearch.rollover.DefaultIndexNameProvider;
import com.netflix.conductor.elasticsearch.rollover.IndexManager;
import com.netflix.conductor.elasticsearch.rollover.IndexManagerDAOV5;
import com.netflix.conductor.elasticsearch.rollover.IndexManagerRestDAOV5;
import com.netflix.conductor.elasticsearch.rollover.IndexNameProvider;
import com.netflix.conductor.elasticsearch.rollover.RolloverIndexProvider;
import com.netflix.conductor.elasticsearch.rollover.listener.DefaultRetryListenerProvider;
import com.netflix.conductor.elasticsearch.rollover.listener.RetryListenerProvider;
import com.netflix.conductor.elasticsearch.rollover.listener.RollOverRetryListenerProvider;

import java.util.HashSet;
import java.util.Set;


/**
 * @author Viren
 * Provider for the elasticsearch index DAO.
 */
public class ElasticSearchV5Module extends AbstractModule {

    private boolean restTransport;
    private boolean isRolloverIndexingEnabled;

    public ElasticSearchV5Module(ElasticSearchConfiguration elasticSearchConfiguration) {

        Set<String> REST_SCHEMAS = new HashSet<>();
        REST_SCHEMAS.add("http");
        REST_SCHEMAS.add("https");

        String esTransport = elasticSearchConfiguration.getURIs().get(0).getScheme();

        this.restTransport = REST_SCHEMAS.contains(esTransport);
        this.isRolloverIndexingEnabled = elasticSearchConfiguration.isRolloverIndexingEnabled();
    }

    @Override
    protected void configure() {

        if(isRolloverIndexingEnabled)
        {
            bind(IndexNameProvider.class).to(RolloverIndexProvider.class).asEagerSingleton();
            bind(RetryListenerProvider.class).to(RollOverRetryListenerProvider.class).asEagerSingleton();
        }
        else
        {
            bind(IndexNameProvider.class).to(DefaultIndexNameProvider.class).asEagerSingleton();
            bind(RetryListenerProvider.class).to(DefaultRetryListenerProvider.class).asEagerSingleton();
        }

        if (restTransport) {
            bind(IndexManager.class).to(IndexManagerRestDAOV5.class);
            bind(IndexDAO.class).to(ElasticSearchRestDAOV5.class);
        } else {
            bind(IndexManager.class).to(IndexManagerDAOV5.class);
            bind(IndexDAO.class).to(ElasticSearchDAOV5.class);
        }

        bind(EmbeddedElasticSearchProvider.class).to(EmbeddedElasticSearchV5Provider.class);
    }
}
