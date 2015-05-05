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

import org.elasticsearch.action.admin.cluster.ClusterAction;
import org.elasticsearch.client.ClusterAdminClient;

public class FlushAction extends ClusterAction<FlushRequest, FlushResponse, FlushRequestBuilder> {

    public static final FlushAction INSTANCE = new FlushAction();

    public static final String NAME = "cluster:admin/auditplugin/flush";

    private FlushAction() {
        super(NAME);
    }

    @Override
    public FlushResponse newResponse() {
        return new FlushResponse();
    }

    @Override
    public FlushRequestBuilder newRequestBuilder(final ClusterAdminClient client) {
        return new FlushRequestBuilder(client);
    }
}