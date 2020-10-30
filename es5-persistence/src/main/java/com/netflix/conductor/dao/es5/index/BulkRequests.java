package com.netflix.conductor.dao.es5.index;

class BulkRequests<K> {
    private final long lastFlushTime;
    private final K bulkRequestWrapper;

    long getLastFlushTime() {
        return lastFlushTime;
    }

    K getBulkRequest() {
        return bulkRequestWrapper;
    }

    BulkRequests(long lastFlushTime, K bulkRequestWrapper) {
        this.lastFlushTime = lastFlushTime;
        this.bulkRequestWrapper = bulkRequestWrapper;
    }
}