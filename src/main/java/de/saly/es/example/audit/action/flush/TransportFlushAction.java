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

import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesOperationAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

import de.saly.es.example.audit.service.AuditService;

public class TransportFlushAction extends TransportNodesOperationAction<FlushRequest, FlushResponse, NodeFlushRequest, NodeFlushResponse> {

    private AuditService auditService;

    @Inject
    public TransportFlushAction(final Settings settings, final String actionName, final ClusterName clusterName,
            final ThreadPool threadPool, final ClusterService clusterService, final TransportService transportService,
            final ActionFilters actionFilters) {
        super(settings, actionName, clusterName, threadPool, clusterService, transportService, actionFilters);
        //HandledTransportAction
        this.transportService.registerHandler(FlushAction.NAME, new FlushRequestHandler());

    }

    public void setAuditService(final AuditService auditService) {
        this.auditService = auditService;
    }

    @Override
    protected String executor() {
        return "management";
    }

    @Override
    protected FlushRequest newRequest() {
        return new FlushRequest();
    }

    @Override
    protected FlushResponse newResponse(final FlushRequest request, final AtomicReferenceArray nodesResponses) {
        final List<NodeFlushResponse> nodes = Lists.<NodeFlushResponse> newArrayList();
        for (int i = 0; i < nodesResponses.length(); i++) {
            final Object resp = nodesResponses.get(i);
            if ((resp instanceof NodeFlushResponse)) {
                nodes.add((NodeFlushResponse) resp);
            }
        }
        return new FlushResponse(this.clusterName, nodes.toArray(new NodeFlushResponse[nodes.size()]));

    }

    @Override
    protected NodeFlushRequest newNodeRequest() {
        return new NodeFlushRequest();
    }

    @Override
    protected NodeFlushRequest newNodeRequest(final String nodeId, final FlushRequest request) {
        return new NodeFlushRequest(request, nodeId);
    }

    @Override
    protected NodeFlushResponse newNodeResponse() {
        return new NodeFlushResponse();
    }

    @Override
    protected NodeFlushResponse nodeOperation(final NodeFlushRequest request) throws ElasticsearchException {
        final int count = flush();
        return new NodeFlushResponse(this.clusterService.localNode(), count);
    }

    private int flush() {
        return auditService.flush();
    }

    @Override
    protected boolean accumulateExceptions() {
        return false;
    }

    private class FlushRequestHandler extends BaseTransportRequestHandler<FlushRequest> {

        @Override
        public FlushRequest newInstance() {
            return new FlushRequest();
        }

        @Override
        public String executor() {
            return "management";
        }

        @Override
        public final void messageReceived(final FlushRequest request, final TransportChannel channel) throws Exception {
            request.listenerThreaded(false);
            execute(request, new ActionListener<FlushResponse>() {
                @Override
                public void onResponse(final FlushResponse response) {
                    try {
                        channel.sendResponse(response);
                    } catch (final Throwable e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(final Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (final Exception e1) {
                        logger.warn("Failed to send error response for action [{}] and request [{}]", actionName, request, e1);
                    }
                }
            });
        }
    }
}
