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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.client.ClusterAdminClient;

public class FlushRequestBuilder extends NodesOperationRequestBuilder<FlushRequest, FlushResponse, FlushRequestBuilder> {

    public FlushRequestBuilder(final ClusterAdminClient client) {
        super(client, new FlushRequest());
    }

    @Override
    protected void doExecute(final ActionListener<FlushResponse> listener) {
        client.execute(FlushAction.INSTANCE, request, listener);
    }

}
