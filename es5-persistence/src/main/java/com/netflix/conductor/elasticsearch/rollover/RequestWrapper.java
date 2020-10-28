package com.netflix.conductor.elasticsearch.rollover;

public class RequestWrapper<T> {
    private T request;
    private final Long createdTime;

    public T getRequest() {
        return request;
    }

    public void updateRequest(T request) {
        this.request = request;
    }

    public Long getCreatedTime() {
        return createdTime;
    }

    public RequestWrapper(T request, Long createdTime) {
        this.request = request;
        this.createdTime = createdTime;
    }
}
