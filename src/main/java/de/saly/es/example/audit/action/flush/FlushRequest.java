/*
Copyright 2015 Hendrik Saly

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package de.saly.es.example.audit.action.flush;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.nodes.NodesOperationRequest;

public class FlushRequest extends NodesOperationRequest<FlushRequest> {

    public FlushRequest(final ActionRequest request, final String... nodesIds) {
        super(request, nodesIds);
    }

    public FlushRequest(final String... nodesIds) {
        super(nodesIds);
    }

    public FlushRequest() {
        super();
    }

    @Override
    public ActionRequestValidationException validate() {
        return null; //no need to validate anything here
    }

}