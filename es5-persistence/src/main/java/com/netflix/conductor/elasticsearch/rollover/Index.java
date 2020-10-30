package com.netflix.conductor.elasticsearch.rollover;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class Index implements Serializable, Comparable<Index> {

    private final String name;
    private final long createdTimestamp;

    public String getName() {
        return name;
    }

    public long getCreatedTimestamp() {
        return createdTimestamp;
    }

    @JsonCreator
    public Index(@JsonProperty("i") String name,
                 @JsonProperty("creation.date") long createdTimestamp) {
        this.name = name;
        this.createdTimestamp = createdTimestamp;
    }

    @Override
    public int compareTo(Index other) {
        //Compare the index based on the index creation time
        return Long.compare(this.createdTimestamp, other.createdTimestamp);
    }
}
