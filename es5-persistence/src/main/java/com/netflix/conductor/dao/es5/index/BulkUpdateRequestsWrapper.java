package com.netflix.conductor.dao.es5.index;

import com.netflix.conductor.elasticsearch.rollover.RequestWrapper;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BulkUpdateRequestsWrapper {
    private List<RequestWrapper<UpdateRequest>> updateRequests;

    BulkUpdateRequestsWrapper() {
        updateRequests = Collections.synchronizedList(new ArrayList<>());
    }

    public void addRequest(RequestWrapper<UpdateRequest> request) {
        synchronized (this) {
            updateRequests.add(request);
        }
    }

    public List<RequestWrapper<UpdateRequest>> getUpdateRequests() {
        return updateRequests;
    }

    public void setUpdateRequestWrapperPojos(List<RequestWrapper<UpdateRequest>> requests) {
        this.updateRequests = requests;
    }

    public BulkRequest constructBulkRequest() {
        BulkRequest request = new BulkRequest();
        for (RequestWrapper<UpdateRequest> updateRequest : updateRequests) {
            request.add(updateRequest.getRequest());
        }

        return request;
    }

    public int numberOfActions() {
        return updateRequests.size();
    }
}
