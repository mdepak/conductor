package com.netflix.conductor.elasticsearch.rollover;

import java.util.Optional;

public interface IndexManager {

    /**
     *
     * @return
     */
    IndexNameProvider getIndexNameProvider();

    Optional<String> getCurrentAliasIndexName();
}
