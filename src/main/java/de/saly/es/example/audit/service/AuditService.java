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

package de.saly.es.example.audit.service;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.netty.util.internal.ConcurrentHashMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine.Create;
import org.elasticsearch.index.engine.Engine.Delete;
import org.elasticsearch.index.engine.Engine.DeleteByQuery;
import org.elasticsearch.index.engine.Engine.Index;
import org.elasticsearch.index.indexing.IndexingOperationListener;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;

import de.saly.es.example.audit.action.flush.TransportFlushAction;

public class AuditService extends AbstractLifecycleComponent<AuditService> {

    public static final String SETTING_AUDIT_INDEX_NAME = "audit.index_name";
    public static final String SETTING_AUDIT_CAPTURE_SETTINGS_CHANGES = "audit.capture_settings_changes";

    public static final String DEFAULT_AUDIT_INDEX_NAME = "audit";
    public static final String AUDIT_INDEX_INDEXING_TYPE = "indexevents";
    private final String auditIndexName;
    private final boolean captureSettingsChanges;
    private final IndicesService indicesService;
    private final Client client;
    private final ClusterService clusterService;
    private volatile boolean isMaster = false;
    private final BulkProcessor bulk;
    private final AtomicInteger pendingBulkItemCount = new AtomicInteger();
    private Thread poller;

    private final IndicesLifecycle.Listener auditIndicesLsListener = new IndicesLifecycle.Listener() {

        private final ConcurrentHashMap<ShardId, AuditIndexOpListener> listeners = new ConcurrentHashMap<ShardId, AuditService.AuditIndexOpListener>();

        @Override
        public void afterIndexCreated(final IndexService indexService) {
            if (isMaster && !indexService.index().name().equals(auditIndexName)) {
                recordIndexCreate(indexService);
            }
        }

        @Override
        public void afterIndexDeleted(final org.elasticsearch.index.Index index, final Settings indexSettings) {
            if (isMaster && !index.name().equals(auditIndexName)) {
                recordIndexDelete(index);
            }
        }

        @Override
        public void afterIndexShardStarted(final IndexShard indexShard) {

            if (indexShard.routingEntry().primary() && !indexShard.indexService().index().name().equals(auditIndexName)) {

                final AuditIndexOpListener auditListener = new AuditIndexOpListener(indexShard);

                if (captureSettingsChanges) {
                    indexShard.indexService().settingsService().addListener(auditListener);
                }

                indexShard.indexingService().addListener(auditListener);

                listeners.put(indexShard.shardId(), auditListener);

                logger.info("Listener for shard {} added", indexShard.shardId());
            }
        }

        @Override
        public void beforeIndexShardClosed(final ShardId shardId, final IndexShard indexShard, final Settings indexSettings) {
            final AuditIndexOpListener listener = listeners.remove(shardId);

            if (listener != null) {
                indexShard.indexingService().removeListener(listener);
                logger.info("Listener for shard {} removed", shardId);
            }
        };
    };

    @Inject
    public AuditService(final Settings settings, final IndicesService indicesService, final Client client,
            final ClusterService clusterService, final TransportFlushAction tfa) {
        super(settings);
        auditIndexName = settings.get(SETTING_AUDIT_INDEX_NAME, DEFAULT_AUDIT_INDEX_NAME);
        captureSettingsChanges = settings.getAsBoolean(SETTING_AUDIT_CAPTURE_SETTINGS_CHANGES, false);
        this.indicesService = indicesService;
        this.client = client;
        this.clusterService = clusterService;
        tfa.setAuditService(this);
        bulk = BulkProcessor.builder(client, new BulkProcessor.Listener() {

            @Override
            public void beforeBulk(final long executionId, final BulkRequest request) {
            }

            @Override
            public void afterBulk(final long executionId, final BulkRequest request, final Throwable failure) {
                logger.error("Bulk error", failure);
                pendingBulkItemCount.addAndGet(-request.numberOfActions());
            }

            @Override
            public void afterBulk(final long executionId, final BulkRequest request, final BulkResponse response) {
                pendingBulkItemCount.addAndGet(-response.getItems().length);
            }
        }).setBulkActions(100).setConcurrentRequests(0)
        //.setBulkSize()
        //.setFlushInterval(TimeValue.timeValueSeconds(1))
        .build();

        this.clusterService.addLifecycleListener(new LifecycleListener() {

            @Override
            public void afterStart() {

                client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute(new ActionListener<ClusterHealthResponse>() {
                    @Override
                    public void onResponse(final ClusterHealthResponse response) {
                        if (response.getStatus() == ClusterHealthStatus.RED) {
                            logger.error("The cluster is not available. The status is RED.");
                        } else {
                            logger.info("cluster ok, will check index");
                            checkAuditIndex();

                        }
                    }

                    @Override
                    public void onFailure(final Throwable e) {
                        logger.error("The cluster is not available.", e);
                    }
                });

            }
        });
    }

    private void createAuditIndex() {

        logger.info("will create audit index");

        final Settings auditIndexSettings = ImmutableSettings.builder().put(SETTING_NUMBER_OF_SHARDS, 3).put(SETTING_NUMBER_OF_REPLICAS, 1)
                .build();

        final XContentBuilder auditIndexEventsTypeMapping = Change.getMapping();

        client.admin().indices().prepareCreate(auditIndexName).addMapping(AUDIT_INDEX_INDEXING_TYPE, auditIndexEventsTypeMapping)
        .setSettings(auditIndexSettings).execute(new ActionListener<CreateIndexResponse>() {
            @Override
            public void onResponse(final CreateIndexResponse response) {
                if (!response.isAcknowledged()) {
                    logger.error("Failed to create {}.", auditIndexName);
                    throw new ElasticsearchException("Failed to create index " + auditIndexName);
                }
            }

            @Override
            public void onFailure(final Throwable e) {
                logger.error("Failed to create {}", e, auditIndexName);
            }
        });
    }

    private void checkAuditIndex() {

        client.admin().indices().prepareExists(auditIndexName).execute(new ActionListener<IndicesExistsResponse>() {
            @Override
            public void onResponse(final IndicesExistsResponse response) {
                if (response.isExists()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("{} index exists already.", auditIndexName);
                    }
                } else {
                    createAuditIndex();
                }
            }

            @Override
            public void onFailure(final Throwable e) {
                logger.error("The state of {} index is invalid.", e, auditIndexName);
            }
        });
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        logger.info("doStart()");

        poller = new Thread(new Runnable() {

            @Override
            public void run() {
                while (true) {
                    try {
                        final IndexRequest req = queue.take();
                        if (req == null) {
                            break;
                        }
                        bulk.add(req);
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                logger.info("poller thread died");
            }
        });
        poller.setDaemon(true);
        poller.setName("poller");
        poller.start();

        this.clusterService.add(new ClusterStateListener() {

            @Override
            public void clusterChanged(final ClusterChangedEvent event) {

                if (event.localNodeMaster()) {
                    isMaster = true;
                } else {
                    isMaster = false;
                }

            }
        });

        this.indicesService.indicesLifecycle().addListener(auditIndicesLsListener);

    }

    @Override
    protected void doStop() throws ElasticsearchException {
        logger.info("doStop()");
        shutdown();
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        logger.info("doClose()");
        shutdown();
    }

    private void shutdown() {
        flush();

        this.indicesService.indicesLifecycle().removeListener(auditIndicesLsListener);

        try {
            bulk.awaitClose(5, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        try {
            queue.add(null);

            try {
                poller.join(1000);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }

        } catch (final IllegalStateException ise) {
            logger.warn("Cannot shutdown poller thread", ise);
        }

    }

    public int flush() {

        while (queue.size() > 0) {
            LockSupport.parkNanos(1000 * 50);
        }

        final int count = pendingBulkItemCount.get();
        bulk.flush();

        //if ConcurrentRequests > 0 we ll wait here because close() triggers a flush which is async
        while (pendingBulkItemCount.get() > 0) {
            LockSupport.parkNanos(1000 * 50);
        }

        logger.info("flushing " + count + " ...");
        return count;
    }

    //TODO capture alias changes too
    protected void recordIndexCreate(final IndexService indexService) {
        if (indexService == null) {
            return;
        }

        final String nodeName = indexService.nodeName();
        final String indexName = indexService.index().name();
        final Settings indexSettings = indexService.settingsService().getSettings();
        BytesReference settingsAsBytesReference = null;
        try {
            settingsAsBytesReference = indexSettings
                    .toXContent(XContentBuilder.builder(JsonXContent.jsonXContent), ToXContent.EMPTY_PARAMS).bytes();
        } catch (final IOException e) {
            logger.error("Unable to convert to bytes reference", e);
        }

        final Change change = new Change(nodeName, indexName, null, null, settingsAsBytesReference, -1, Change.ChangeType.INDEX_CREATE);
        storeChange(change);
    }

    protected void recordIndexDelete(final org.elasticsearch.index.Index index) {
        if (index == null) {
            return;
        }

        final String indexName = index.name();

        final Change change = new Change(null, indexName, null, null, null, -1, Change.ChangeType.INDEX_DELETE);
        storeChange(change);
    }

    protected void recordDataChange(final DeleteByQuery operation, final IndexShard indexShard) {
        if (operation == null) {
            return;
        }

        final String nodeName = indexShard.nodeName();
        final String indexName = indexShard.indexService().index().name();

        final Change change = new Change(nodeName, indexName, operation.types(), null, operation.source(), -1,
                Change.ChangeType.DATA_DELETE);
        storeChange(change);
    }

    protected void recordDataChange(final Create operation, final IndexShard indexShard) {
        if (operation == null) {
            return;
        }

        final String nodeName = indexShard.nodeName();
        final String indexName = indexShard.indexService().index().name();

        final Change change = new Change(nodeName, indexName, new String[] { operation.type() }, operation.id(), operation.source(),
                operation.version(), Change.ChangeType.DATA_CREATE);
        storeChange(change);
    }

    protected void recordDataChange(final Delete operation, final IndexShard indexShard) {
        if (operation == null) {
            return;
        }

        final String nodeName = indexShard.nodeName();
        final String indexName = indexShard.indexService().index().name();

        final Change change = new Change(nodeName, indexName, new String[] { operation.type() }, operation.id(), null, operation.version(),
                Change.ChangeType.DATA_DELETE);
        storeChange(change);
    }

    protected void recordDataChange(final Index operation, final IndexShard indexShard) {
        if (operation == null) {
            return;
        }

        final String nodeName = indexShard.nodeName();
        final String indexName = indexShard.indexService().index().name();

        final Change change = new Change(nodeName, indexName, new String[] { operation.type() }, operation.id(), operation.source(),
                operation.version(), Change.ChangeType.DATA_CREATE);
        storeChange(change);
    }

    protected void recordIndexSettingsChange(final Settings settings, final IndexShard indexShard) {
        final String nodeName = indexShard.nodeName();
        final String indexName = indexShard.indexService().index().name();
        BytesReference settingsAsBytesReference = null;
        try {
            settingsAsBytesReference = settings.toXContent(XContentBuilder.builder(JsonXContent.jsonXContent), ToXContent.EMPTY_PARAMS)
                    .bytes();
        } catch (final IOException e) {
            logger.error("Unable to convert to bytes reference", e);
        }

        final Change change = new Change(nodeName, indexName, null, null, settingsAsBytesReference, -1, Change.ChangeType.INDEX_SETTINGS);
        storeChange(change);
    }

    private final LinkedBlockingQueue<IndexRequest> queue = new LinkedBlockingQueue<IndexRequest>();

    protected void storeChange(final Change change) {

        try {
            queue.put(new IndexRequest(auditIndexName).type(AUDIT_INDEX_INDEXING_TYPE).source(change.event));
            pendingBulkItemCount.addAndGet(1);
        } catch (final InterruptedException e) {

            e.printStackTrace();
        }

        //logger.info("{}", change);
    }

    protected static class Change {

        private final Map<String, Object> event = new HashMap<String, Object>();

        public enum ChangeType {
            DATA_CREATE, INDEX_CREATE, DATA_DELETE, INDEX_DELETE, INDEX_SETTINGS;
        };

        public Change(final String nodeName, final String index, final String[] type, final String id, final BytesReference jsonContent,
                final long version, final ChangeType changeType) {
            super();
            event.put("@timestamp", new Date());
            event.put("node_name", nodeName);
            event.put("index", index);
            event.put("type", type == null ? null : Arrays.asList(type));
            event.put("id", id);
            try {
                event.put("content", jsonContent == null ? null : XContentHelper.convertToJson(jsonContent, false));
            } catch (final IOException e) {
                event.put("content", e.toString());
            }
            event.put("version", version);
            event.put("change_type", changeType.toString());
        }

        public static XContentBuilder getMapping() {
            try {
                return XContentBuilder.builder(JsonXContent.jsonXContent).startObject().startObject(AUDIT_INDEX_INDEXING_TYPE)
                        .startObject("properties")

                        // @timestamp
                        .startObject("@timestamp").field("type", "date").field("format", "dateOptionalTime").endObject()

                        // node_name
                        .startObject("node_name").field("type", "string").field("index", "not_analyzed").endObject()

                        // index
                        .startObject("index").field("type", "string").field("index", "not_analyzed").endObject()

                        // type
                        .startObject("type").field("type", "string").field("index", "not_analyzed").endObject()

                        // id
                        .startObject("id").field("type", "string").field("index", "not_analyzed").endObject()

                        // content
                        .startObject("content").field("type", "string").endObject()

                        // version
                        .startObject("version").field("type", "long").endObject()

                        // change_type
                        .startObject("change_type").field("type", "string").field("index", "not_analyzed").endObject()

                        .endObject().endObject().endObject()

                        ;
            } catch (final IOException e) {
                e.printStackTrace();
                return null;
            }
        }

        @Override
        public String toString() {
            return "Change [event=" + event + "]";
        }
    }

    private class AuditIndexOpListener extends IndexingOperationListener implements IndexSettingsService.Listener {

        private final IndexShard indexShard;

        public AuditIndexOpListener(final IndexShard indexShard) {
            super();
            this.indexShard = indexShard;
        }

        @Override
        public void postIndex(final Index index) {
            recordDataChange(index, indexShard);
        }

        @Override
        public void postDelete(final Delete delete) {
            recordDataChange(delete, indexShard);
        }

        @Override
        public void postDeleteByQuery(final DeleteByQuery deleteByQuery) {
            recordDataChange(deleteByQuery, indexShard);
        }

        @Override
        public void postCreate(final Create create) {
            recordDataChange(create, indexShard);
        }

        @Override
        public void onRefreshSettings(final Settings settings) {
            recordIndexSettingsChange(settings, indexShard);

        }
    }
}
