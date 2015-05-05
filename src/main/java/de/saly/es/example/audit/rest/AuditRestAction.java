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

package de.saly.es.example.audit.rest;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.action.support.RestToXContentListener;

import de.saly.es.example.audit.action.flush.FlushAction;
import de.saly.es.example.audit.action.flush.FlushRequest;
import de.saly.es.example.audit.action.flush.FlushResponse;

public class AuditRestAction extends BaseRestHandler {

    @Inject
    public AuditRestAction(final Settings settings, final Client client, final RestController controller) {
        super(settings, controller, client);
        controller.registerHandler(Method.GET, "/_cluster/_auditflush", this);
        controller.registerHandler(Method.POST, "/_cluster/_auditflush", this);

    }

    @Override
    protected void handleRequest(final RestRequest request, final RestChannel channel, final Client client) throws Exception {
        final FlushRequest flushRequest = new FlushRequest();
        client.admin().cluster().execute(FlushAction.INSTANCE, flushRequest, new RestToXContentListener<FlushResponse>(channel));
    }

}
