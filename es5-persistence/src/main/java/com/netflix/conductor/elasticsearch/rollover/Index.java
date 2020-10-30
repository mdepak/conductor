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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * Represents the Elasticsearch Index
 */
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
