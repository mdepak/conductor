package com.netflix.conductor.elasticsearch.rollover;

import com.netflix.conductor.elasticsearch.ElasticSearchConfiguration;

import javax.inject.Inject;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class RolloverIndexProvider implements IndexNameProvider {

    private TreeMap<Long, String> creationTimeToNameMap;
    private final ElasticSearchConfiguration configuration;
    private static final TimeZone GMT = TimeZone.getTimeZone("GMT");
    private static final SimpleDateFormat CONDUCTOR_INDEX_DATE_FORMAT= new SimpleDateFormat("yyyy-MM-dd-HH-mm");

    static {
        CONDUCTOR_INDEX_DATE_FORMAT.setTimeZone(GMT);
    }

    @Inject
    public RolloverIndexProvider(ElasticSearchConfiguration config){
        this.configuration = config;
    }

    @Override
    public String getIndexNameToCreate() {
        return configuration.getIndexName() + "_" + CONDUCTOR_INDEX_DATE_FORMAT.format(new Date()) + "-1";
    }

    @Override
    public String getWriteIndexName(int attempt, long createdTime) {
        Long currentLower = creationTimeToNameMap.lowerKey(createdTime - 30 * 1000); // Timeout seconds for the rollover request

        int cur = 1;
        while (currentLower != null && cur < attempt) {
            currentLower = creationTimeToNameMap.higherKey(currentLower);
            cur++;
        }

        if (currentLower == null) {
            return null;
        }

        return creationTimeToNameMap.get(currentLower);
    }

    @Override
    public String getLookupRequestIndexName(int attempt) {
        Long curIndex = creationTimeToNameMap.lastKey();
        int idx = 1;
        while (curIndex != null && idx < attempt) {
            curIndex = creationTimeToNameMap.higherKey(curIndex);
        }

        return creationTimeToNameMap.get(curIndex);
    }

    @Override
    public String getSearchRequestIndexName() {
        return configuration.getIndexName() + "*";
    }

    @Override
    public void updateIndices(List<Index> indices) {
        //TODO: Check need for synchronization
        this.creationTimeToNameMap = new TreeMap<>(indices.stream().collect(Collectors.toMap(Index::getCreatedTimestamp, Index::getName)));
    }
}
