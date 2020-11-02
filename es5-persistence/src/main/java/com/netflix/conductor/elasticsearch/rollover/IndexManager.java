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

public interface IndexManager {

    /**
     * @return the index provider for the index requests
     */
    IndexNameProvider getIndexNameProvider();

    /**
     * @return boolean value whether alias index exists
     */
    boolean isAliasIndexExists();

    /**
     * Updates the indices maintained by the index manager
     */
    void updateIndex();
}
