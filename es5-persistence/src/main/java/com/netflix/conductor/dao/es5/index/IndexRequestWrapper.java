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
