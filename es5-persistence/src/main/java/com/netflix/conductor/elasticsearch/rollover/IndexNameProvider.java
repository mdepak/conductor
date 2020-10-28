package com.netflix.conductor.elasticsearch.rollover;

import java.util.List;

public interface IndexNameProvider {


    String getIndexNameToCreate();

    /**
     *
     * @param attempt
     * @param createdTime
     * @return
     */
    String getWriteIndexName(int attempt, long createdTime);

    /**
     *
     * @param attempt
     * @return
     */
    String getLookupRequestIndexName(int attempt);

    /**
     *
     * @return
     */
    String getSearchRequestIndexName();

    /**
     *
     * @param indexList
     */
    default void updateIndices(List<Index> indexList){
    }
}
