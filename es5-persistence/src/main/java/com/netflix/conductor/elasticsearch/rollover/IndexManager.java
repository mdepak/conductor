package com.netflix.conductor.elasticsearch.rollover;

public interface IndexManager {

    /**
     *
     * @return
     */
    IndexNameProvider getIndexNameProvider();

    boolean isAliasIndexExists();

    void updateIndex();
}
