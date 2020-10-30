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
package com.netflix.conductor.dao.es5.index;

import com.netflix.conductor.elasticsearch.rollover.RequestWrapper;

import javax.annotation.Nullable;
import java.util.function.Function;
import java.util.function.Supplier;

public class IndexRequestWrapper<T, R> implements Supplier<R> {

    private final RequestWrapper<T> requestWrapper;
    private final Function<T, R> requestFunction;

    public T getRequest() {
        return requestWrapper.getRequest();
    }

    public void setRequest(T request) {
        this.requestWrapper.updateRequest(request);
    }

    IndexRequestWrapper(T request, Function<T, R> requestFunction, @Nullable Long createdTime) {
        this.requestWrapper = new RequestWrapper<>(request, createdTime);
        this.requestFunction = requestFunction;
    }

    public Long getCreatedTime() {
        return requestWrapper.getCreatedTime();
    }

    @Override
    public R get() {
        return requestFunction.apply(requestWrapper.getRequest());
    }
}
