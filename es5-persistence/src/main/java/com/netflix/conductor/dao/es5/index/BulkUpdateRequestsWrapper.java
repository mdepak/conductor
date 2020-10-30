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
